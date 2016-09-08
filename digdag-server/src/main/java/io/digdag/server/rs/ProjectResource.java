package io.digdag.server.rs;

import java.util.List;
import java.util.stream.Collectors;
import java.net.URI;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestRevision;
import io.digdag.client.api.RestSecretList;
import io.digdag.client.api.RestSecretMetadata;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSetSecretRequest;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.TempFileManager;
import io.digdag.core.TempFileManager.TempDir;
import io.digdag.core.TempFileManager.TempFile;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.core.repository.ArchiveType;
import io.digdag.core.repository.Project;
import io.digdag.core.repository.ProjectControl;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionWithLastAttempt;
import io.digdag.core.storage.ArchiveManager;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.server.GenericJsonExceptionHandler;
import io.digdag.spi.SecretControlStore;
import io.digdag.spi.SecretControlStoreManager;
import io.digdag.spi.SecretScopes;
import io.digdag.client.api.SecretValidation;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.spi.StorageObject;
import io.digdag.util.Md5CountInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.digdag.server.rs.RestModels.sessionModels;
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
    // GET  /api/projects/{id}/workflows/<name>          # lookup a workflow of a project by name
    // GET  /api/projects/{id}/sessions                  # list sessions for a project
    // GET  /api/projects/{id}/sessions?workflow<name>   # list sessions for a workflow in the project
    // GET  /api/projects/{id}/archive                   # download archive file of the latest revision of a project
    // GET  /api/projects/{id}/archive?revision=<name>   # download archive file of a former revision of a project
    // PUT  /api/projects?project=<name>&revision=<name> # create a new revision (also create a project if it doesn't exist)
    // GET  /api/projects/{id}/secrets                   # list secrets for a project
    // PUT  /api/projects/{id}/secrets/<key>             # set a secret for a project
    // DEL  /api/projects/{id}/secrets/<key>             # delete a secret for a project
    //
    // Deprecated:
    // GET  /api/project?name=<name>                     # lookup a project by name
    // GET  /api/projects/{id}/workflow?name=name        # lookup a workflow of a project by name
    // GET  /api/projects/{id}/workflow?name=name&revision=name    # lookup a workflow of a past revision of a project by name

    private static final int ARCHIVE_TOTAL_SIZE_LIMIT = 2 * 1024 * 1024;
    private static final int ARCHIVE_FILE_SIZE_LIMIT = ARCHIVE_TOTAL_SIZE_LIMIT;

    private final ConfigFactory cf;
    private final YamlConfigLoader rawLoader;
    private final WorkflowCompiler compiler;
    private final ArchiveManager archiveManager;
    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final SchedulerManager srm;
    private final TempFileManager tempFiles;
    private final SessionStoreManager ssm;
    private final SecretControlStoreManager scsp;

    @Inject
    public ProjectResource(
            ConfigFactory cf,
            YamlConfigLoader rawLoader,
            WorkflowCompiler compiler,
            ArchiveManager archiveManager,
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            SchedulerManager srm,
            TempFileManager tempFiles,
            SessionStoreManager ssm,
            SecretControlStoreManager scsp)
    {
        this.cf = cf;
        this.rawLoader = rawLoader;
        this.srm = srm;
        this.compiler = compiler;
        this.archiveManager = archiveManager;
        this.rm = rm;
        this.sm = sm;
        this.tempFiles = tempFiles;
        this.ssm = ssm;
        this.scsp = scsp;
    }

    private static StoredProject ensureNotDeletedProject(StoredProject proj)
        throws ResourceNotFoundException
    {
        if (proj.getDeletedAt().isPresent()) {
            throw new ResourceNotFoundException(String.format(ENGLISH,
                    "Project id=%d is already deleted at %s",
                    proj.getId(), proj.getDeletedAt().get().toString()));
        }
        return proj;
    }

    @GET
    @Path("/api/project")
    public RestProject getProject(@QueryParam("name") String name)
        throws ResourceNotFoundException
    {
        Preconditions.checkArgument(name != null, "name= is required");

        ProjectStore ps = rm.getProjectStore(getSiteId());
        StoredProject proj = ensureNotDeletedProject(ps.getProjectByName(name));
        StoredRevision rev = ps.getLatestRevision(proj.getId());
        return RestModels.project(proj, rev);
    }

    @GET
    @Path("/api/projects")
    public List<RestProject> getProjects(@QueryParam("name") String name)
        throws ResourceNotFoundException
    {
        ProjectStore ps = rm.getProjectStore(getSiteId());

        if (name != null) {
            try {
                StoredProject proj = ensureNotDeletedProject(ps.getProjectByName(name));
                StoredRevision rev = ps.getLatestRevision(proj.getId());
                return ImmutableList.of(RestModels.project(proj, rev));
            }
            catch (ResourceNotFoundException ex) {
                return ImmutableList.of();
            }
        }
        else {
            // TODO fix n-m db access
            return ps.getProjects(100, Optional.absent())
                .stream()
                .map(proj -> {
                    try {
                        StoredRevision rev = ps.getLatestRevision(proj.getId());
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
        ProjectStore ps = rm.getProjectStore(getSiteId());
        StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId));
        StoredRevision rev = ps.getLatestRevision(proj.getId());
        return RestModels.project(proj, rev);
    }

    @GET
    @Path("/api/projects/{id}/revisions")
    public List<RestRevision> getRevisions(@PathParam("id") int projId, @QueryParam("last_id") Integer lastId)
        throws ResourceNotFoundException
    {
        ProjectStore ps = rm.getProjectStore(getSiteId());
        StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId));
        List<StoredRevision> revs = ps.getRevisions(proj.getId(), 100, Optional.fromNullable(lastId));
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

        ProjectStore ps = rm.getProjectStore(getSiteId());
        StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId));

        StoredRevision rev;
        if (revName == null) {
            rev = ps.getLatestRevision(proj.getId());
        }
        else {
            rev = ps.getRevisionByName(proj.getId(), revName);
        }
        StoredWorkflowDefinition def = ps.getWorkflowDefinitionByName(rev.getId(), name);

        return RestModels.workflowDefinition(proj, rev, def);
    }

    @GET
    @Path("/api/projects/{id}/workflows/{name}")
    public RestWorkflowDefinition getWorkflowByName(@PathParam("id") int projId, @PathParam("name") String name, @QueryParam("revision") String revName)
        throws ResourceNotFoundException
    {
        return getWorkflow(projId, name, revName);
    }

    @GET
    @Path("/api/projects/{id}/workflows")
    public List<RestWorkflowDefinition> getWorkflows(
            @PathParam("id") int projId,
            @QueryParam("revision") String revName,
            @QueryParam("name") String name)
        throws ResourceNotFoundException
    {
        ProjectStore ps = rm.getProjectStore(getSiteId());
        StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId));

        StoredRevision rev;
        if (revName == null) {
            rev = ps.getLatestRevision(proj.getId());
        }
        else {
            rev = ps.getRevisionByName(proj.getId(), revName);
        }

        if (name != null) {
            try {
                StoredWorkflowDefinition def = ps.getWorkflowDefinitionByName(rev.getId(), name);
                return ImmutableList.of(RestModels.workflowDefinition(proj, rev, def));
            }
            catch (ResourceNotFoundException ex) {
                return ImmutableList.of();
            }
        }
        else {
            // TODO should here support pagination?
            List<StoredWorkflowDefinition> defs = ps.getWorkflowDefinitions(rev.getId(), Integer.MAX_VALUE, Optional.absent());

            return defs.stream()
                .map(def -> RestModels.workflowDefinition(proj, rev, def))
                .collect(Collectors.toList());
        }
    }

    @GET
    @Path("/api/projects/{id}/sessions")
    public List<RestSession> getSessions(
            @PathParam("id") int projectId,
            @QueryParam("workflow") String workflowName,
            @QueryParam("last_id") Long lastId)
        throws ResourceNotFoundException
    {
        ProjectStore ps = rm.getProjectStore(getSiteId());
        SessionStore ss = ssm.getSessionStore(getSiteId());

        StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projectId));

        List<StoredSessionWithLastAttempt> sessions;
        if (workflowName != null) {
            sessions = ss.getSessionsOfWorkflowByName(proj.getId(), workflowName, 100, Optional.fromNullable(lastId));
        } else {
            sessions = ss.getSessionsOfProject(proj.getId(), 100, Optional.fromNullable(lastId));
        }

        return sessionModels(ps, sessions);
    }

    @GET
    @Path("/api/projects/{id}/archive")
    @Produces("application/gzip")
    public Response getArchive(@PathParam("id") int projId, @QueryParam("revision") String revName)
        throws ResourceNotFoundException, StorageFileNotFoundException
    {
        ProjectStore ps = rm.getProjectStore(getSiteId());
        ensureNotDeletedProject(ps.getProjectById(projId));

        Optional<ArchiveManager.StoredArchive> archiveOrNone =
            archiveManager.getArchive(ps, projId, revName);
        if (!archiveOrNone.isPresent()) {
            throw new ResourceNotFoundException("Archive is not stored");
        }
        else {
            ArchiveManager.StoredArchive archive = archiveOrNone.get();

            // TODO DigdagClient doesn't follow redirection. It should be fixed.
            //Optional<DirectDownloadHandle> direct = archive.getDirectDownloadHandle();
            //if (direct.isPresent()) {
            //    try {
            //        return Response.seeOther(URI.create(direct.get().getUrl())).build();
            //    }
            //    catch (IllegalArgumentException ex) {
            //        // pass-through if invalid url
            //    }
            //}

            Optional<byte[]> bytes = archive.getByteArray();
            if (bytes.isPresent()) {
                return Response.ok(bytes.get()).build();
            }

            return Response.ok(new StreamingOutput() {
                @Override
                public void write(OutputStream out)
                    throws IOException, WebApplicationException
                {
                    StorageObject obj;
                    try {
                        obj = archive.open();
                    }
                    catch (StorageFileNotFoundException ex) {
                        // throwing StorageFileNotFoundException should become 404 Not Found
                        // to be consistent with the case of DirectDownloadHandle
                        throw new NotFoundException(
                                GenericJsonExceptionHandler
                                .toResponse(Response.Status.NOT_FOUND, "Archive file not found"));
                    }
                    try (InputStream in = obj.getContentInputStream()) {
                        ByteStreams.copy(in, out);
                    }
                }
            }).build();
        }
    }

    @DELETE
    @Path("/api/projects/{id}")
    public RestProject deleteProject(@PathParam("id") int projId)
        throws ResourceNotFoundException
    {
        ProjectStore ps = rm.getProjectStore(getSiteId());
        return ProjectControl.deleteProject(ps, projId, (control, proj) -> {
            StoredRevision rev = ps.getLatestRevision(proj.getId());
            return RestModels.project(proj, rev);
        });
    }

    @PUT
    @Consumes("application/gzip")
    @Path("/api/projects")
    public RestProject putProject(@QueryParam("project") String name, @QueryParam("revision") String revision,
            InputStream body, @HeaderParam("Content-Length") long contentLength,
            @QueryParam("schedule_from") String scheduleFromString)
        throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        Preconditions.checkArgument(name != null, "project= is required");
        Preconditions.checkArgument(revision != null, "revision= is required");

        Instant scheduleFrom;
        if (scheduleFromString == null || scheduleFromString.isEmpty()) {
            scheduleFrom = Instant.now();
        }
        else {
            try {
                scheduleFrom = Instant.parse(scheduleFromString);
            }
            catch (DateTimeParseException ex) {
                throw new IllegalArgumentException("Invalid schedule_from= parameter format. Expected yyyy-MM-dd'T'HH:mm:ss'Z' format", ex);
            }
        }

        if (contentLength > ARCHIVE_TOTAL_SIZE_LIMIT) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Size of the uploaded archive file exceeds limit (%d bytes)",
                        ARCHIVE_TOTAL_SIZE_LIMIT));
        }
        int size = (int) contentLength;

        try (TempFile tempFile = tempFiles.createTempFile("upload-", ".tar.gz")) {
            // Read uploaded data to the temp file and following variables
            ArchiveMetadata meta;
            byte[] md5;
            try (OutputStream writeToTemp = Files.newOutputStream(tempFile.get())) {
                Md5CountInputStream md5Count = new Md5CountInputStream(body);
                meta = readArchiveMetadata(new DuplicateInputStream(md5Count, writeToTemp), name);
                md5 = md5Count.getDigest();
                if (md5Count.getCount() != contentLength) {
                    throw new IllegalArgumentException("Content-Length header doesn't match with uploaded data size");
                }
            }

            ArchiveManager.Location location =
                archiveManager.newArchiveLocation(getSiteId(), name, revision, size);
            boolean storeInDb = location.getArchiveType().equals(ArchiveType.DB);

            if (!storeInDb) {
                // upload to storage
                try {
                    archiveManager
                        .getStorage(location.getArchiveType())
                        .put(location.getPath(), size, () -> Files.newInputStream(tempFile.get()));
                }
                catch (RuntimeException | IOException ex) {
                    throw new InternalServerErrorException("Failed to upload archive to a remote storage", ex);
                }
            }

            // Getting secrets might fail. To avoid ending up with a project without secrets, get the secrets _before_ storing the project.
            // If getting the project secrets fails, the project will not be stored and the push can then be retried with the same revision.
            Map<String, String> secrets = getSecrets().get();

            RestProject restProject = rm.getProjectStore(getSiteId()).putAndLockProject(
                    Project.of(name),
                    (store, storedProject) -> {
                        ProjectControl lockedProj = new ProjectControl(store, storedProject);
                        StoredRevision rev;
                        if (storeInDb) {
                            // store data in db
                            byte[] data = new byte[size];
                            try (InputStream in = Files.newInputStream(tempFile.get())) {
                                ByteStreams.readFully(in, data);
                            }
                            catch (RuntimeException | IOException ex) {
                                throw new InternalServerErrorException("Failed to load archive data in memory", ex);
                            }
                            rev = lockedProj.insertRevision(
                                    Revision.builderFromArchive(revision, meta, getUserInfo())
                                            .archiveType(ArchiveType.DB)
                                            .archivePath(Optional.absent())
                                            .archiveMd5(Optional.of(md5))
                                            .build()
                            );
                            lockedProj.insertRevisionArchiveData(rev.getId(), data);
                        }
                        else {
                            // store location of the uploaded file in db
                            rev = lockedProj.insertRevision(
                                    Revision.builderFromArchive(revision, meta, getUserInfo())
                                            .archiveType(location.getArchiveType())
                                            .archivePath(Optional.of(location.getPath()))
                                            .archiveMd5(Optional.of(md5))
                                            .build()
                            );
                        }

                        List<StoredWorkflowDefinition> defs =
                            lockedProj.insertWorkflowDefinitions(rev,
                                    meta.getWorkflowList().get(),
                                    srm, scheduleFrom);
                        return RestModels.project(storedProject, rev);
                    });

            SecretControlStore secretControlStore = scsp.getSecretControlStore(getSiteId());
            secrets.forEach((k, v) -> secretControlStore.setProjectSecret(restProject.getId(), SecretScopes.PROJECT_DEFAULT, k, v));
            return restProject;
        }
    }

    private ArchiveMetadata readArchiveMetadata(InputStream in, String projectName)
        throws IOException
    {
        try (TempDir dir = tempFiles.createTempDir("push", projectName)) {
            long totalSize = 0;
            try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(in, 32*1024)))) {
                totalSize = extractConfigFiles(dir.get(), archive);
            }

            if (totalSize > ARCHIVE_TOTAL_SIZE_LIMIT) {
                throw new IllegalArgumentException(String.format(ENGLISH,
                            "Total size of the archive exceeds limit (%d > %d bytes)",
                            totalSize, ARCHIVE_TOTAL_SIZE_LIMIT));
            }

            Config renderedConfig = rawLoader.loadFile(
                    dir.child(ArchiveMetadata.FILE_NAME).toFile()).toConfig(cf);
            return renderedConfig.convert(ArchiveMetadata.class);
        }
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

    private void validateTarEntry(TarArchiveEntry entry)
    {
        if (entry.getSize() > ARCHIVE_FILE_SIZE_LIMIT) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Size of a file in the archive exceeds limit (%d > %d bytes): %s",
                        entry.getSize(), ARCHIVE_FILE_SIZE_LIMIT, entry.getName()));
        }
    }

    @PUT
    @Consumes("application/json")
    @Path("/api/projects/{id}/secrets/{key}")
    public void putProjectSecret(@PathParam("id") int projectId, @PathParam("key") String key, RestSetSecretRequest request)
            throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        if (!SecretValidation.isValidSecret(key, request.value())) {
            throw new IllegalArgumentException("Invalid secret");
        }

        // Verify that the project exists
        ProjectStore projectStore = rm.getProjectStore(getSiteId());
        StoredProject project = projectStore.getProjectById(projectId);
        ensureNotDeletedProject(project);

        SecretControlStore store = scsp.getSecretControlStore(getSiteId());

        store.setProjectSecret(projectId, SecretScopes.PROJECT, key, request.value());
    }

    @DELETE
    @Path("/api/projects/{id}/secrets/{key}")
    public void deleteProjectSecret(@PathParam("id") int projectId, @PathParam("key") String key)
            throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        if (!SecretValidation.isValidSecretKey(key)) {
            throw new IllegalArgumentException("Invalid secret");
        }

        // Verify that the project exists
        ProjectStore projectStore = rm.getProjectStore(getSiteId());
        StoredProject project = projectStore.getProjectById(projectId);
        ensureNotDeletedProject(project);

        SecretControlStore store = scsp.getSecretControlStore(getSiteId());

        store.deleteProjectSecret(projectId, SecretScopes.PROJECT, key);
    }

    @GET
    @Path("/api/projects/{id}/secrets")
    @Produces("application/json")
    public RestSecretList getProjectSecrets(@PathParam("id") int projectId)
            throws IOException, ResourceConflictException, ResourceNotFoundException
    {
        // Verify that the project exists
        ProjectStore projectStore = rm.getProjectStore(getSiteId());
        StoredProject project = projectStore.getProjectById(projectId);
        ensureNotDeletedProject(project);

        SecretControlStore store = scsp.getSecretControlStore(getSiteId());
        List<String> keys = store.listProjectSecrets(projectId, SecretScopes.PROJECT);

        return RestSecretList.builder()
                .secrets(keys.stream().map(RestSecretMetadata::of).collect(Collectors.toList()))
                .build();
    }
}
