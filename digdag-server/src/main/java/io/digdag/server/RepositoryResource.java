package io.digdag.server;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.time.Instant;
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
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.yaml.YamlConfigLoader;
import io.digdag.spi.ScheduleTime;
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
    // [ ] GET  /api/repositories/{id}/revisions                 # list revisions of a repository from recent to old
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
    private final ScheduleStoreManager sm;
    private final SchedulerManager srm;
    private final TempFileManager temp;

    private int siteId = 0;  // TODO get site id from context

    @Inject
    public RepositoryResource(
            ConfigFactory cf,
            YamlConfigLoader rawLoader,
            WorkflowCompiler compiler,
            RepositoryStoreManager rm,
            ScheduleStoreManager sm,
            SchedulerManager srm,
            TempFileManager temp)
    {
        this.cf = cf;
        this.rawLoader = rawLoader;
        this.srm = srm;
        this.compiler = compiler;
        this.rm = rm;
        this.sm = sm;
        this.temp = temp;
    }

    @GET
    @Path("/api/repositories")
    public List<RestRepository> getRepositories()
        throws ResourceNotFoundException
    {
        // TODO n-m db access
        RepositoryStore rs = rm.getRepositoryStore(siteId);
        return rs.getRepositories(100, Optional.absent())
            .stream()
            .map(repo -> {
                try {
                    StoredRevision rev = rs.getLatestRevision(repo.getId());
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
            rev = rs.getLatestRevision(repo.getId());
        }
        else {
            rev = rs.getRevisionByName(repo.getId(), revName);
        }
        return RestModels.repository(repo, rev);
    }

    @GET
    @Path("/api/repositories/{id}/workflows")
    public List<RestWorkflowDefinition> getWorkflows(@PathParam("id") int repoId, @QueryParam("revision") String revName)
        throws ResourceNotFoundException
    {
        // TODO paging
        RepositoryStore rs = rm.getRepositoryStore(siteId);
        StoredRepository repo = rs.getRepositoryById(repoId);

        StoredRevision rev;
        if (revName == null) {
            rev = rs.getLatestRevision(repo.getId());
        }
        else {
            rev = rs.getRevisionByName(repo.getId(), revName);
        }
        List<StoredWorkflowDefinition> defs = rs.getWorkflowDefinitions(rev.getId(), 100, Optional.absent());

        Map<Long, Schedule> scheds = getWorkflowScheduleMap();

        return defs.stream()
            .map(def -> RestModels.workflowDefinition(
                        repo, rev, def,
                        Optional.fromNullable(scheds.get(def.getId())).transform(sched -> ScheduleTime.of(sched.getNextRunTime(), sched.getNextScheduleTime()))
                        ))
            .collect(Collectors.toList());
    }

    private Map<Long, Schedule> getWorkflowScheduleMap()
    {
        List<StoredSchedule> schedules = sm.getScheduleStore(siteId)
            .getSchedules(Integer.MAX_VALUE, Optional.absent());
        ImmutableMap.Builder<Long, Schedule> builder = ImmutableMap.builder();
        for (Schedule schedule : schedules) {
            builder.put(schedule.getWorkflowDefinitionId(), schedule);
        }
        return builder.build();
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
            rev = rs.getLatestRevision(repo.getId());
        }
        else {
            rev = rs.getRevisionByName(repo.getId(), revName);
        }
        return rs.getRevisionArchiveData(rev.getId());
    }

    @PUT
    @Consumes("application/x-gzip")
    @Path("/api/repositories")
    public RestRepository putRepository(@QueryParam("repository") String name, @QueryParam("revision") String revision,
            InputStream body)
        throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        byte[] data = ByteStreams.toByteArray(body);

        ArchiveMetadata meta;
        try (TempDir dir = temp.createTempDir()) {
            try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new ByteArrayInputStream(data)))) {
                extractConfigFiles(dir.get(), archive);
            }
            // jinja is disabled
            Config renderedConfig = rawLoader.loadFile(
                    dir.child(ArchiveMetadata.FILE_NAME),
                    Optional.absent(), Optional.absent());
            meta = renderedConfig.convert(ArchiveMetadata.class);
        }

        RestRepository stored = rm.getRepositoryStore(siteId).putAndLockRepository(
                Repository.of(name),
                (store, storedRepo) -> {
                    RepositoryControl lockedRepo = new RepositoryControl(store, storedRepo);
                    StoredRevision rev = lockedRepo.putRevision(
                            Revision.revisionBuilder()
                                .name(revision)
                                .defaultParams(meta.getDefaultParams())
                                .archiveType("db")
                                .archivePath(Optional.absent())
                                .archiveMd5(Optional.of(calculateArchiveMd5(data)))
                                .build()
                            );
                    lockedRepo.insertRevisionArchiveData(rev.getId(), data);
                    List<StoredWorkflowDefinition> defs =
                        lockedRepo.insertWorkflowDefinitions(rev,
                                meta.getWorkflowList().get(),
                                srm, Instant.now());
                    return RestModels.repository(storedRepo, rev);
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
