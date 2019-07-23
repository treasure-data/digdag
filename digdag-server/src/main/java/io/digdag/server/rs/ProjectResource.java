package io.digdag.server.rs;

import java.util.ArrayList;
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

import com.google.common.base.Strings;
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
import io.digdag.client.api.RestProjectCollection;
import io.digdag.client.api.RestRevision;
import io.digdag.client.api.RestRevisionCollection;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleCollection;
import io.digdag.client.api.RestSecretList;
import io.digdag.client.api.RestSecretMetadata;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionCollection;
import io.digdag.client.api.RestSetSecretRequest;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowDefinitionCollection;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.TempFileManager;
import io.digdag.core.TempFileManager.TempDir;
import io.digdag.core.TempFileManager.TempFile;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.core.database.TransactionManager;
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
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.core.schedule.ScheduleStore;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionWithLastAttempt;
import io.digdag.core.storage.ArchiveManager;
import io.digdag.core.workflow.Workflow;
import io.digdag.core.workflow.WorkflowCompiler;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.server.GenericJsonExceptionHandler;
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.SecretControlStore;
import io.digdag.spi.SecretControlStoreManager;
import io.digdag.spi.SecretScopes;
import io.digdag.client.api.SecretValidation;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.spi.StorageObject;
import io.digdag.util.Md5CountInputStream;
import io.swagger.annotations.Api;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static java.util.Locale.ENGLISH;

@Api("Project")
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
    // GET  /api/projects/{id}/schedules?                # list schedules of the latest revision of a project
    // GET  /api/projects/{id}/schedules?workflow={name} # get the schedule of the latest revision of a workflow in a project
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

    private static final Logger logger = LoggerFactory.getLogger(ProjectResource.class);
    private static int MAX_ARCHIVE_TOTAL_SIZE_LIMIT;
    private static final int DEFAULT_ARCHIVE_TOTAL_SIZE_LIMIT = 2 * 1024 * 1024;
    // TODO: we may want to limit bytes of one file for `MAX_ARCHIVE_FILE_SIZE_LIMIT ` in the future instead of total size limit.
    // See also: https://github.com/treasure-data/digdag/pull/994#discussion_r258402647
    private static int MAX_ARCHIVE_FILE_SIZE_LIMIT;
    private static int MAX_SESSIONS_PAGE_SIZE;
    private static final int DEFAULT_SESSIONS_PAGE_SIZE = 100;

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
    private final TransactionManager tm;
    private final ProjectArchiveLoader projectArchiveLoader;

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
            SecretControlStoreManager scsp,
            TransactionManager tm,
            ProjectArchiveLoader projectArchiveLoader,
            Config systemConfig)
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
        this.tm = tm;
        this.scsp = scsp;
        this.projectArchiveLoader = projectArchiveLoader;
        MAX_SESSIONS_PAGE_SIZE = systemConfig.get("api.max_sessions_page_size", Integer.class, DEFAULT_SESSIONS_PAGE_SIZE);
        MAX_ARCHIVE_TOTAL_SIZE_LIMIT = systemConfig.get("api.max_archive_total_size_limit", Integer.class, DEFAULT_ARCHIVE_TOTAL_SIZE_LIMIT);
        MAX_ARCHIVE_FILE_SIZE_LIMIT = MAX_ARCHIVE_TOTAL_SIZE_LIMIT;
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
        return tm.begin(() -> {
            Preconditions.checkArgument(name != null, "name= is required");

            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectByName(name));
            StoredRevision rev = ps.getLatestRevision(proj.getId());
            return RestModels.project(proj, rev);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/projects")
    public RestProjectCollection getProjects(@QueryParam("name") String name)
    {
        return tm.begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());

            List<RestProject> collection;
            if (name != null) {
                try {
                    StoredProject proj = ensureNotDeletedProject(ps.getProjectByName(name));
                    StoredRevision rev = ps.getLatestRevision(proj.getId());
                    collection = ImmutableList.of(RestModels.project(proj, rev));
                }
                catch (ResourceNotFoundException ex) {
                    collection = ImmutableList.of();
                }
            }
            else {
                // TODO fix n-m db access
                collection = ps.getProjects(100, Optional.absent())
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

            return RestModels.projectCollection(collection);
        });
    }

    @GET
    @Path("/api/projects/{id}")
    public RestProject getProject(@PathParam("id") int projId)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId));
            StoredRevision rev = ps.getLatestRevision(proj.getId());
            return RestModels.project(proj, rev);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/projects/{id}/revisions")
    public RestRevisionCollection getRevisions(@PathParam("id") int projId, @QueryParam("last_id") Integer lastId)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId));
            List<StoredRevision> revs = ps.getRevisions(proj.getId(), 100, Optional.fromNullable(lastId));
            return RestModels.revisionCollection(proj, revs);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/projects/{id}/workflow")
    public RestWorkflowDefinition getWorkflow(@PathParam("id") int projId, @QueryParam("name") String name, @QueryParam("revision") String revName)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
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
        }, ResourceNotFoundException.class);
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
    public RestWorkflowDefinitionCollection getWorkflows(
            @PathParam("id") int projId,
            @QueryParam("revision") String revName,
            @QueryParam("name") String name)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId));

            StoredRevision rev;
            if (revName == null) {
                rev = ps.getLatestRevision(proj.getId());
            }
            else {
                rev = ps.getRevisionByName(proj.getId(), revName);
            }

            List<StoredWorkflowDefinition> collection;
            if (name != null) {
                try {
                    StoredWorkflowDefinition def = ps.getWorkflowDefinitionByName(rev.getId(), name);
                    collection = ImmutableList.of(def);
                }
                catch (ResourceNotFoundException ex) {
                    collection = ImmutableList.of();
                }
            }
            else {
                // TODO should here support pagination?
                collection = ps.getWorkflowDefinitions(rev.getId(), Integer.MAX_VALUE, Optional.absent());
            }
            return RestModels.workflowDefinitionCollection(proj, rev, collection);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/projects/{id}/schedules")
    public RestScheduleCollection getSchedules(
            @PathParam("id") int projectId,
            @QueryParam("workflow") String workflowName,
            @QueryParam("last_id") Integer lastId)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            ProjectStore projectStore = rm.getProjectStore(getSiteId());
            ScheduleStore scheduleStore = sm.getScheduleStore(getSiteId());

            ensureNotDeletedProject(projectStore.getProjectById(projectId));

            List<StoredSchedule> scheds;
            if (workflowName != null) {
                StoredSchedule sched;
                try {
                    sched = scheduleStore.getScheduleByProjectIdAndWorkflowName(projectId, workflowName);
                    scheds = ImmutableList.of(sched);
                }
                catch (ResourceNotFoundException e) {
                    scheds = ImmutableList.of();
                }
            }
            else {
                scheds = scheduleStore.getSchedulesByProjectId(projectId, 100, Optional.fromNullable(lastId));
            }

            return RestModels.scheduleCollection(projectStore, scheds);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/projects/{id}/sessions")
    public RestSessionCollection getSessions(
            @PathParam("id") int projectId,
            @QueryParam("workflow") String workflowName,
            @QueryParam("last_id") Long lastId,
            @QueryParam("page_size") Integer pageSize,
            @QueryParam("page_number") Integer pageNumber)
            throws ResourceNotFoundException
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_SESSIONS_PAGE_SIZE, DEFAULT_SESSIONS_PAGE_SIZE);

        return tm.begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            SessionStore ss = ssm.getSessionStore(getSiteId());

            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projectId));
            Integer page = Optional.fromNullable(pageNumber).or(1);

            Integer totalSessionsCount = ss.getTotalProjectSessionsCount(Optional.fromNullable(lastId), projectId);
            int totalPageCount = totalSessionsCount / validPageSize;
            if (totalSessionsCount % validPageSize != 0) totalPageCount += 1;

            List<StoredSessionWithLastAttempt> sessions;
            if (workflowName != null) {
                sessions = ss.getSessionsOfWorkflowByName(proj.getId(), workflowName, validPageSize, Optional.fromNullable(lastId), page);
            } else {
                sessions = ss.getSessionsOfProject(proj.getId(), validPageSize, Optional.fromNullable(lastId), page);
            }

            return RestModels.sessionCollection(ps, sessions, totalPageCount);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/projects/{id}/archive")
    @Produces("application/gzip")
    public Response getArchive(@PathParam("id") int projId, @QueryParam("revision") String revName)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            ensureNotDeletedProject(ps.getProjectById(projId));

            Optional<ArchiveManager.StoredArchive> archiveOrNone =
                    archiveManager.getArchive(ps, projId, revName);
            if (!archiveOrNone.isPresent()) {
                throw new ResourceNotFoundException("Archive is not stored");
            }
            else {
                ArchiveManager.StoredArchive archive = archiveOrNone.get();

                Optional<DirectDownloadHandle> direct = archive.getDirectDownloadHandle();
                if (direct.isPresent()) {
                    try {
                        return Response.seeOther(URI.create(String.valueOf(direct.get().getUrl()))).build();
                    }
                    catch (IllegalArgumentException ex) {
                        logger.warn("Failed to create a HTTP response to redirect /api/projects/{id}/archive to a direct download URL. " +
                                "Falling back to fetching from the server.", ex);
                    }
                }

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
        }, ResourceNotFoundException.class);
    }

    @DELETE
    @Path("/api/projects/{id}")
    public RestProject deleteProject(@PathParam("id") int projId)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            return ProjectControl.deleteProject(ps, projId, (control, proj) -> {
                StoredRevision rev = ps.getLatestRevision(proj.getId());
                return RestModels.project(proj, rev);
            });
        }, ResourceNotFoundException.class);
    }

    @PUT
    @Consumes("application/gzip")
    @Path("/api/projects")
    public RestProject putProject(@QueryParam("project") String name, @QueryParam("revision") String revision,
            InputStream body, @HeaderParam("Content-Length") long contentLength,
            @QueryParam("schedule_from") String scheduleFromString)
            throws ResourceConflictException, IOException, ResourceNotFoundException
    {
        return tm.<RestProject, IOException, ResourceConflictException, ResourceNotFoundException>begin(() -> {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "project= is required");
            Preconditions.checkArgument(!Strings.isNullOrEmpty(revision), "revision= is required");

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

            if (contentLength > MAX_ARCHIVE_TOTAL_SIZE_LIMIT) {
                throw new IllegalArgumentException(String.format(ENGLISH,
                        "Size of the uploaded archive file exceeds limit (%d bytes)",
                        MAX_ARCHIVE_TOTAL_SIZE_LIMIT));
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

                    validateWorkflowAndSchedule(meta);
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
                secrets.forEach((k, v) -> secretControlStore.setProjectSecret(
                        RestModels.parseProjectId(restProject.getId()),
                        SecretScopes.PROJECT_DEFAULT,
                        k, v));
                return restProject;
            }
        }, IOException.class, ResourceConflictException.class, ResourceNotFoundException.class);
    }

    private ArchiveMetadata readArchiveMetadata(InputStream in, String projectName)
        throws IOException
    {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(projectName), "projectName");
        try (TempDir dir = tempFiles.createTempDir("push", projectName)) {
            long totalSize = 0;
            try (TarArchiveInputStream archive = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(in, 32*1024)))) {
                totalSize = extractConfigFiles(dir.get(), archive);
            }

            if (totalSize > MAX_ARCHIVE_TOTAL_SIZE_LIMIT) {
                throw new IllegalArgumentException(String.format(ENGLISH,
                            "Total size of the archive exceeds limit (%d > %d bytes)",
                            totalSize, MAX_ARCHIVE_TOTAL_SIZE_LIMIT));
            }

            ProjectArchive archive = projectArchiveLoader.load(dir.get(), WorkflowResourceMatcher.defaultMatcher(), cf.create());

            return archive.getArchiveMetadata();
        }
    }

    // TODO: only write .dig files
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
        if (entry.getSize() > MAX_ARCHIVE_FILE_SIZE_LIMIT) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                        "Size of a file in the archive exceeds limit (%d > %d bytes): %s",
                        entry.getSize(), MAX_ARCHIVE_FILE_SIZE_LIMIT, entry.getName()));
        }
    }

    private void validateWorkflowAndSchedule(ArchiveMetadata meta)
    {
        WorkflowDefinitionList defs = meta.getWorkflowList();
        for (WorkflowDefinition def : defs.get()) {
            Workflow wf = compiler.compile(def.getName(), def.getConfig());

            // validate workflow and schedule
            for (WorkflowTask task : wf.getTasks()) {
                // raise an exception if task doesn't valid.
                task.getConfig();
            }
            Revision rev = Revision.builderFromArchive("check", meta, getUserInfo())
                    .archiveType(ArchiveType.NONE)
                    .build();
            // raise an exception if "schedule:" is invalid.
            srm.tryGetScheduler(rev, def);

        }
    }

    @PUT
    @Consumes("application/json")
    @Path("/api/projects/{id}/secrets/{key}")
    public void putProjectSecret(@PathParam("id") int projectId, @PathParam("key") String key, RestSetSecretRequest request)
            throws ResourceNotFoundException
    {
        tm.begin(() -> {
            if (!SecretValidation.isValidSecret(key, request.value())) {
                throw new IllegalArgumentException("Invalid secret");
            }

            // Verify that the project exists
            ProjectStore projectStore = rm.getProjectStore(getSiteId());
            StoredProject project = projectStore.getProjectById(projectId);
            ensureNotDeletedProject(project);

            SecretControlStore store = scsp.getSecretControlStore(getSiteId());

            store.setProjectSecret(projectId, SecretScopes.PROJECT, key, request.value());
            return null;
        }, ResourceNotFoundException.class);
    }

    @DELETE
    @Path("/api/projects/{id}/secrets/{key}")
    public void deleteProjectSecret(@PathParam("id") int projectId, @PathParam("key") String key)
            throws ResourceNotFoundException
    {
        tm.begin(() -> {
            if (!SecretValidation.isValidSecretKey(key)) {
                throw new IllegalArgumentException("Invalid secret");
            }

            // Verify that the project exists
            ProjectStore projectStore = rm.getProjectStore(getSiteId());
            StoredProject project = projectStore.getProjectById(projectId);
            ensureNotDeletedProject(project);

            SecretControlStore store = scsp.getSecretControlStore(getSiteId());

            store.deleteProjectSecret(projectId, SecretScopes.PROJECT, key);
            return null;
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/projects/{id}/secrets")
    @Produces("application/json")
    public RestSecretList getProjectSecrets(@PathParam("id") int projectId)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            // Verify that the project exists
            ProjectStore projectStore = rm.getProjectStore(getSiteId());
            StoredProject project = projectStore.getProjectById(projectId);
            ensureNotDeletedProject(project);

            SecretControlStore store = scsp.getSecretControlStore(getSiteId());
            List<String> keys = store.listProjectSecrets(projectId, SecretScopes.PROJECT);

            return RestSecretList.builder()
                    .secrets(keys.stream().map(RestSecretMetadata::of).collect(Collectors.toList()))
                    .build();
        }, ResourceNotFoundException.class);
    }
}
