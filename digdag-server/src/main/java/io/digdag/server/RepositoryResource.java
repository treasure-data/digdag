package io.digdag.server;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigFactory;
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.yaml.YamlConfigLoader;
import io.digdag.client.api.*;
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
    // [*] GET  /api/repositories/{id}?revision=name             # show a former revision of a repository
    // [*] GET  /api/repositories/{id}/workflows                 # list workflows of the latest revision of a repository
    // [*] GET  /api/repositories/{id}/workflows?revision=name   # list workflows of a former revision of a repository
    // [*] GET  /api/repositories/{id}/archive                   # download archive file of the latest revision of a repository
    // [*] GET  /api/repositories/{id}/archive?revision=name     # download archive file of a former revision of a repository
    // [*] PUT  /api/repositories?repository=<name>&revision=<name>  # create a new revision (also create a repository if it doesn't exist)

    private final ConfigFactory cf;
    private final YamlConfigLoader rawLoader;
    private final WorkflowCompiler compiler;
    private final RepositoryStoreManager rm;
    private final SchedulerManager scheds;
    private final TempFileManager temp;

    private int siteId = 0;  // TODO get site id from context

    @Inject
    public RepositoryResource(
            ConfigFactory cf,
            YamlConfigLoader rawLoader,
            WorkflowCompiler compiler,
            RepositoryStoreManager rm,
            SchedulerManager scheds,
            TempFileManager temp)
    {
        this.cf = cf;
        this.rawLoader = rawLoader;
        this.scheds = scheds;
        this.compiler = compiler;
        this.rm = rm;
        this.temp = temp;
    }

    @GET
    @Path("/api/repositories")
    public List<RestRepository> getRepositories(@QueryParam("revision") String revName)
        throws ResourceNotFoundException
    {
        // TODO paging
        // TODO n-m db access
        RepositoryStore rs = rm.getRepositoryStore(siteId);
        return rs.getRepositories(100, Optional.absent())
            .stream()
            .map(repo -> {
                try {
                    StoredRevision rev;
                    if (revName == null) {
                        rev = rs.getLatestActiveRevision(repo.getId());
                    }
                    else {
                        rev = rs.getRevisionByName(repo.getId(), revName);
                    }
                    return RestModels.repository(repo, rev);
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
    public RestRepository getRepository(@PathParam("id") int repoId, @QueryParam("revision") String revName)
        throws ResourceNotFoundException
    {
        RepositoryStore rs = rm.getRepositoryStore(siteId);
        StoredRepository repo = rs.getRepositoryById(repoId);
        StoredRevision rev;
        if (revName == null) {
            rev = rs.getLatestActiveRevision(repo.getId());
        }
        else {
            rev = rs.getRevisionByName(repo.getId(), revName);
        }
        return RestModels.repository(repo, rev);
    }

    @GET
    @Path("/api/repositories/{id}/workflows")
    public List<RestWorkflow> getWorkflows(@PathParam("id") int repoId, @QueryParam("revision") String revName)
        throws ResourceNotFoundException
    {
        // TODO paging
        RepositoryStore rs = rm.getRepositoryStore(siteId);
        StoredRepository repo = rs.getRepositoryById(repoId);
        StoredRevision rev;
        if (revName == null) {
            rev = rs.getLatestActiveRevision(repo.getId());
        }
        else {
            rev = rs.getRevisionByName(repo.getId(), revName);
        }
        List<StoredWorkflowSource> workflows = rs.getWorkflowSources(rev.getId(), 100, Optional.absent());

        return workflows.stream()
            .map(workflow -> RestModels.workflow(repo, rev, workflow))
            .collect(Collectors.toList());
    }

    @GET
    @Path("/api/repositories/{id}/archive")
    @Produces("application/x-gzip")
    public byte[] getArchive(@PathParam("id") int repoId, @QueryParam("revision") String revName)
        throws ResourceNotFoundException
    {
        RepositoryStore rs = rm.getRepositoryStore(siteId);
        StoredRepository repo = rs.getRepositoryById(repoId);
        StoredRevision rev;
        if (revName == null) {
            rev = rs.getLatestActiveRevision(repo.getId());
        }
        else {
            rev = rs.getRevisionByName(repo.getId(), revName);
        }
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
            // jinja and !!include are disabled
            Config renderedConfig = rawLoader.loadFile(
                    dir.child(ArchiveMetadata.FILE_NAME),
                    Optional.absent(), Optional.absent());
            meta = renderedConfig.convert(ArchiveMetadata.class);
        }

        RestRepository stored = rm.getRepositoryStore(siteId).putRepository(
                Repository.of(name),
                (RepositoryControl repoControl) -> {
                    StoredRevision rev = repoControl.putRevision(
                            Revision.revisionBuilder()
                                .name(revision)
                                .defaultParams(meta.getDefaultParams())
                                .archiveType("db")
                                .archivePath(Optional.absent())
                                .archiveData(data)
                                .archiveMd5(Optional.of(calculateArchiveMd5(data)))
                                .build()
                            );
                    try {
                        List<StoredWorkflowSource> storedWorkflows =
                            repoControl.insertWorkflowSources(rev.getId(), meta.getWorkflowList().get());
                        List<StoredScheduleSource> storedSchedules =
                            repoControl.insertScheduleSources(rev.getId(), meta.getScheduleList().get());
                        repoControl.syncLatestRevision(rev,
                                storedWorkflows, storedSchedules,
                                scheds, new Date());
                    }
                    catch (ResourceConflictException ex) {
                        throw new IllegalStateException("Database state error", ex);
                    }
                    return RestModels.repository(repoControl.get(), rev);
                });

        return stored;
    }

    // TODO here doesn't have to extract files exception ArchiveMetadata.FILE_NAME
    //      rawLoader.loadFile doesn't have to render the file because it's already rendered.
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
