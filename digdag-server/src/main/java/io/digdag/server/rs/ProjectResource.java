package io.digdag.server.rs;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.time.Instant;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.nio.file.Files;
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
import com.google.common.base.Preconditions;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.core.TempFileManager;
import io.digdag.core.TempFileManager.TempDir;
import io.digdag.spi.ScheduleTime;
import io.digdag.client.api.*;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import static java.util.Locale.ENGLISH;

@Path("/")
@Produces("application/json")
public class ProjectResource
    extends AuthenticatedResource
{
    // GET  /api/projects                                # list projects
    // GET  /api/projects?name=<name>                    # lookup a project by name, or return an empty array
    // GET  /api/projects/{id}                           # show a project
    // GET  /api/projects/{id}/revisions                 # list revisions of a project from recent to old
    // GET  /api/projects/{id}/workflows                 # list workflows of the latest revision of a project
    // GET  /api/projects/{id}/workflows?revision=<name> # list workflows of a past revision of a project
    // GET  /api/projects/{id}/workflows?name=<name>     # lookup a workflow of a project by name
    // GET  /api/projects/{id}/archive                   # download archive file of the latest revision of a project
    // GET  /api/projects/{id}/archive?revision=<name>   # download archive file of a former revision of a project
    // PUT  /api/projects?project=<name>&revision=<name> # create a new revision (also create a project if it doesn't exist)
    //
    // Deprecated:
    // GET  /api/project?name=<name>                     # lookup a project by name
    // GET  /api/projects/{id}/workflow?name=name        # lookup a workflow of a project by name
    // GET  /api/projects/{id}/workflow?name=name&revision=name    # lookup a workflow of a past revision of a project by name

    private static final long ARCHIVE_TOTAL_SIZE_LIMIT = 2 * 1024 * 1024;
    private static final long ARCHIVE_FILE_SIZE_LIMIT = ARCHIVE_TOTAL_SIZE_LIMIT;

    private final ConfigFactory cf;
    private final YamlConfigLoader rawLoader;
    private final WorkflowCompiler compiler;
    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final SchedulerManager srm;
    private final TempFileManager tempFiles;

    @Inject
    public ProjectResource(
            ConfigFactory cf,
            YamlConfigLoader rawLoader,
            WorkflowCompiler compiler,
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            SchedulerManager srm,
            TempFileManager tempFiles)
    {
        this.cf = cf;
        this.rawLoader = rawLoader;
        this.srm = srm;
        this.compiler = compiler;
        this.rm = rm;
        this.sm = sm;
        this.tempFiles = tempFiles;
    }

    @GET
    @Path("/api/project")
    public RestProject getProject(@QueryParam("name") String name)
        throws ResourceNotFoundException
    {
        Preconditions.checkArgument(name != null, "name= is required");

        ProjectStore rs = rm.getProjectStore(getSiteId());
        StoredProject proj = rs.getProjectByName(name);
        StoredRevision rev = rs.getLatestRevision(proj.getId());
        return RestModels.project(proj, rev);
    }

    @GET
    @Path("/api/projects")
    public List<RestProject> getProjects(@QueryParam("name") String name)
        throws ResourceNotFoundException
    {
        ProjectStore rs = rm.getProjectStore(getSiteId());

        if (name != null) {
            try {
                StoredProject proj = rs.getProjectByName(name);
                StoredRevision rev = rs.getLatestRevision(proj.getId());
                return ImmutableList.of(RestModels.project(proj, rev));
            }
            catch (ResourceNotFoundException ex) {
                return ImmutableList.of();
            }
        }
        else {
            // TODO fix n-m db access
            return rs.getProjects(100, Optional.absent())
                .stream()
                .map(proj -> {
                    try {
                        StoredRevision rev = rs.getLatestRevision(proj.getId());
                        return RestModels.project(proj, rev);
                    }
                    catch (ResourceNotFoundException ex) {
                        return null;
                    }
                })
                .filter(proj -> proj != null)
                .collect(Collectors.toList());
        }
    }

    @GET
    @Path("/api/projects/{id}")
    public RestProject getProject(@PathParam("id") int projId)
        throws ResourceNotFoundException
    {
        ProjectStore rs = rm.getProjectStore(getSiteId());
        StoredProject proj = rs.getProjectById(projId);
        StoredRevision rev = rs.getLatestRevision(proj.getId());
        return RestModels.project(proj, rev);
    }

    @GET
    @Path("/api/projects/{id}/revisions")
    public List<RestRevision> getRevisions(@PathParam("id") int projId, @QueryParam("last_id") Integer lastId)
        throws ResourceNotFoundException
    {
        ProjectStore rs = rm.getProjectStore(getSiteId());
        StoredProject proj = rs.getProjectById(projId);
        List<StoredRevision> revs = rs.getRevisions(proj.getId(), 100, Optional.fromNullable(lastId));
        return revs.stream()
            .map(rev -> RestModels.revision(proj, rev))
            .collect(Collectors.toList());
    }

    @GET
    @Path("/api/projects/{id}/workflow")
    public RestWorkflowDefinition getWorkflow(@PathParam("id") int projId, @QueryParam("name") String name, @QueryParam("revision") String revName)
        throws ResourceNotFoundException
    {
        Preconditions.checkArgument(name != null, "name= is required");

        ProjectStore rs = rm.getProjectStore(getSiteId());
        StoredProject proj = rs.getProjectById(projId);

        StoredRevision rev;
        if (revName == null) {
            rev = rs.getLatestRevision(proj.getId());
        }
        else {
            rev = rs.getRevisionByName(proj.getId(), revName);
        }
        StoredWorkflowDefinition def = rs.getWorkflowDefinitionByName(rev.getId(), name);

        return RestModels.workflowDefinition(proj, rev, def);
    }

    @GET
    @Path("/api/projects/{id}/workflows")
    public List<RestWorkflowDefinition> getWorkflows(
            @PathParam("id") int projId,
            @QueryParam("revision") String revName,
            @QueryParam("name") String name)
        throws ResourceNotFoundException
    {
        ProjectStore rs = rm.getProjectStore(getSiteId());
        StoredProject proj = rs.getProjectById(projId);

        StoredRevision rev;
        if (revName == null) {
            rev = rs.getLatestRevision(proj.getId());
        }
        else {
            rev = rs.getRevisionByName(proj.getId(), revName);
        }

        if (name != null) {
            try {
                StoredWorkflowDefinition def = rs.getWorkflowDefinitionByName(rev.getId(), name);
                return ImmutableList.of(RestModels.workflowDefinition(proj, rev, def));
            }
            catch (ResourceNotFoundException ex) {
                return ImmutableList.of();
            }
        }
        else {
            // TODO should here support pagination?
            List<StoredWorkflowDefinition> defs = rs.getWorkflowDefinitions(rev.getId(), Integer.MAX_VALUE, Optional.absent());

            return defs.stream()
                .map(def -> RestModels.workflowDefinition(proj, rev, def))
                .collect(Collectors.toList());
        }
    }

    @GET
    @Path("/api/projects/{id}/archive")
    @Produces("application/gzip")
    public byte[] getArchive(@PathParam("id") int projId, @QueryParam("revision") String revName)
        throws ResourceNotFoundException
    {
        ProjectStore rs = rm.getProjectStore(getSiteId());
        StoredProject proj = rs.getProjectById(projId);
        StoredRevision rev;
        if (revName == null) {
            rev = rs.getLatestRevision(proj.getId());
        }
        else {
            rev = rs.getRevisionByName(proj.getId(), revName);
        }
        return rs.getRevisionArchiveData(rev.getId());
    }

    @PUT
    @Consumes("application/gzip")
    @Path("/api/projects")
    public RestProject putProject(@QueryParam("project") String name, @QueryParam("revision") String revision,
            InputStream body)
        throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        Preconditions.checkArgument(name != null, "project= is required");
        Preconditions.checkArgument(revision != null, "revision= is required");

        byte[] data = ByteStreams.toByteArray(body);

        if (data.length > ARCHIVE_TOTAL_SIZE_LIMIT) {
            // TODO throw this exception before reading all data in memory
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Size of the uploaded archive file exceeds limit (%d bytes)",
                        ARCHIVE_TOTAL_SIZE_LIMIT));
        }

        ArchiveMetadata meta;
        try (TempDir dir = tempFiles.createTempDir("push", name)) {
            long totalSize = 0;
            try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new ByteArrayInputStream(data)))) {
                totalSize = extractConfigFiles(dir.get(), archive);
            }
            if (totalSize > ARCHIVE_TOTAL_SIZE_LIMIT) {
                throw new IllegalArgumentException(String.format(ENGLISH,
                            "Total size of the archive exceeds limit (%d > %d bytes)",
                            totalSize, ARCHIVE_TOTAL_SIZE_LIMIT));
            }

            // jinja is disabled here
            Config renderedConfig = rawLoader.loadFile(
                    dir.child(ArchiveMetadata.FILE_NAME).toFile()).toConfig(cf);
            meta = renderedConfig.convert(ArchiveMetadata.class);
        }

        RestProject stored = rm.getProjectStore(getSiteId()).putAndLockProject(
                Project.of(name),
                (store, storedProject) -> {
                    ProjectControl lockedProj = new ProjectControl(store, storedProject);
                    StoredRevision rev = lockedProj.insertRevision(
                            Revision.builderFromArchive(revision, meta)
                                .archiveType("db")
                                .archivePath(Optional.absent())
                                .archiveMd5(Optional.of(calculateArchiveMd5(data)))
                                .build()
                            );
                    lockedProj.insertRevisionArchiveData(rev.getId(), data);
                    List<StoredWorkflowDefinition> defs =
                        lockedProj.insertWorkflowDefinitions(rev,
                                meta.getWorkflowList().get(),
                                srm, Instant.now());
                    return RestModels.project(storedProject, rev);
                });

        return stored;
    }

    // TODO here doesn't have to extract files exception ArchiveMetadata.FILE_NAME
    //      rawLoader.loadFile doesn't have to render the file because it's already rendered.
    private long extractConfigFiles(java.nio.file.Path dir, TarArchiveInputStream archive)
        throws IOException
    {
        long totalSize = 0;
        TarArchiveEntry entry;
        while (true) {
            entry = archive.getNextTarEntry();
            if (entry == null) {
                break;
            }
            if (entry.isDirectory()) {
                // do nothing
            }
            else {
                validateTarEntry(entry);
                totalSize += entry.getSize();

                java.nio.file.Path file = dir.resolve(entry.getName());
                Files.createDirectories(file.getParent());
                try (OutputStream out = Files.newOutputStream(file)) {
                    ByteStreams.copy(archive, out);
                }
            }
        }
        return totalSize;
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

    private void validateTarEntry(TarArchiveEntry entry)
    {
        if (entry.getSize() > ARCHIVE_FILE_SIZE_LIMIT) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Size of a file in the archive exceeds limit (%d > %d bytes): %s",
                        entry.getSize(), ARCHIVE_FILE_SIZE_LIMIT, entry.getName()));
        }
    }
}
