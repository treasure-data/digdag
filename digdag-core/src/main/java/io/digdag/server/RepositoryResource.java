package io.digdag.server;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
import io.digdag.core.config.*;
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.server.TempFileManager.TempDir;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

@Path("/")
@Produces("application/json")
public class RepositoryResource
{
    // [*] GET  /api/repositories                                # list the latest revisions of repositories
    // [*] GET  /api/repositories/{id}                           # show the latest revision of a repository
    // [ ] GET  /api/repositories/{id}?revision=name             # show a former revision of a repository
    // [*] GET  /api/repositories/{id}/workflows                 # list workflows of the latest revision of a repository
    // [ ] GET  /api/repositories/{id}/workflows?revision=name   # list workflows of a former revision of a repository
    // [*] GET  /api/repositories/{id}/archive                   # download archive file of the latest revision of a repository
    // [ ] GET  /api/repositories/{id}/archive?revision=name     # download archive file of a former revision of a repository
    // [*] PUT  /api/repositories?repository=<name>&revision=<name>  # create a new revision (also create a repository if it doesn't exist)

    private final ConfigFactory cf;
    private final YamlConfigLoader loader;
    private final WorkflowCompiler compiler;
    private final RepositoryStoreManager rm;
    private final ScheduleStoreManager scheduleStore;
    private final SchedulerManager scheds;
    private final TempFileManager temp;

    private int siteId = 0;  // TODO get site id from context

    @Inject
    public RepositoryResource(
            ConfigFactory cf,
            YamlConfigLoader loader,
            WorkflowCompiler compiler,
            RepositoryStoreManager rm,
            ScheduleStoreManager scheduleStore,
            SchedulerManager scheds,
            TempFileManager temp)
    {
        this.cf = cf;
        this.loader = loader;
        this.scheduleStore = scheduleStore;
        this.scheds = scheds;
        this.compiler = compiler;
        this.rm = rm;
        this.temp = temp;
    }

    @GET
    @Path("/api/repositories")
    public List<RestRepository> getRepositories()
            throws ResourceNotFoundException
    {
        // TODO paging
        // TODO n-m db access
        return rm.getRepositoryStore(siteId).getRepositories(100, Optional.absent())
            .stream()
            .map(repo -> {
                try {
                    StoredRevision rev = rm.getRepositoryStore(siteId).getLatestActiveRevision(repo.getId());
                    return RestRepository.of(repo, rev);
                }
                catch (ResourceNotFoundException ex) {
                    return null;
                }
            })
            .filter(repo -> repo != null)
            .collect(Collectors.toList());
    }

    @GET
    @Path("/api/repositories/{id}")
    public RestRepository getRepository(@PathParam("id") int repoId)
            throws ResourceNotFoundException
    {
        StoredRepository repo = rm.getRepositoryStore(siteId).getRepositoryById(repoId);
        StoredRevision rev = rm.getRepositoryStore(siteId).getLatestActiveRevision(repo.getId());
        return RestRepository.of(repo, rev);
    }

    @GET
    @Path("/api/repositories/{id}/workflows")
    public List<RestWorkflow> getWorkflows(@PathParam("id") int repoId)
            throws ResourceNotFoundException
    {
        // TODO paging
        StoredRepository repo = rm.getRepositoryStore(siteId).getRepositoryById(repoId);
        StoredRevision rev = rm.getRepositoryStore(siteId).getLatestActiveRevision(repoId);
        List<StoredWorkflowSource> workflows = rm.getRepositoryStore(siteId).getWorkflows(rev.getId(), 100, Optional.absent());

        return workflows.stream()
            .map(workflow -> RestWorkflow.of(repo, rev, workflow))
            .collect(Collectors.toList());
    }

    @GET
    @Path("/api/repositories/{id}/archive")
    @Produces("application/x-gzip")
    public byte[] getArchive(@PathParam("id") int repoId)
            throws ResourceNotFoundException
    {
        StoredRepository repo = rm.getRepositoryStore(siteId).getRepositoryById(repoId);
        StoredRevision rev = rm.getRepositoryStore(siteId).getLatestActiveRevision(repoId);
        return rev.getArchiveData().get();
    }

    @PUT
    @Consumes("application/x-gzip")
    @Path("/api/repositories")
    public RestRepository putRepository(@QueryParam("repository") String name, @QueryParam("revision") String revision,
            InputStream body)
            throws IOException
    {
        byte[] data = ByteStreams.toByteArray(body);

        ArchiveMetadata meta;
        try (TempDir dir = temp.createTempDir()) {
            try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new ByteArrayInputStream(data)))) {
                extractConfigFiles(dir.get(), archive);
            }
            meta = loader.loadFile(dir.child(ArchiveMetadata.FILE_NAME)).convert(ArchiveMetadata.class);
        }

        RestRepository stored = rm.getRepositoryStore(siteId).putRepository(
                Repository.of(name),
                (RepositoryControl repo) -> {
                    StoredRevision rev = repo.putRevision(
                            Revision.revisionBuilder()
                                .name(revision)
                                .globalParams(cf.create())
                                .archiveType("db")
                                .archivePath(Optional.absent())
                                .archiveData(data)
                                .archiveMd5(Optional.of(calculateArchiveMd5(data)))
                                .build()
                            );
                    try {
                        for (WorkflowSource workflow : meta.getWorkflows().get()) {
                            repo.insertWorkflow(rev.getId(), workflow);
                        }
                        repo.syncSchedulesTo(scheduleStore, scheds, new Date(), rev);
                    }
                    catch (ResourceConflictException ex) {
                        throw new IllegalStateException("Database state error", ex);
                    }
                    return RestRepository.of(repo.get(), rev);
                });

        return stored;
    }

    private List<File> extractConfigFiles(File dir, ArchiveInputStream archive)
            throws IOException
    {
        ImmutableList.Builder<File> files = ImmutableList.builder();
        ArchiveEntry entry;
        while (true) {
            entry = archive.getNextEntry();
            if (entry == null) {
                break;
            }
            if (entry.isDirectory()) {
                // do nothing
            }
            else {
                File file = new File(dir, entry.getName());
                file.getParentFile().mkdirs();
                try (FileOutputStream out = new FileOutputStream(file)) {
                    ByteStreams.copy(archive, out);
                }
                files.add(new File(entry.getName()));
            }
        }
        return files.build();
    }

    private byte[] calculateArchiveMd5(byte[] data)
    {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(data);
        }
        catch (NoSuchAlgorithmException ex) {
            throw Throwables.propagate(ex);
        }
    }
}
