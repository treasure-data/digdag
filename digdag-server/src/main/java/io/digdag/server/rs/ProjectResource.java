package io.digdag.server.rs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.net.URI;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.nio.file.Files;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.Response;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.digdag.client.api.RestProject;
import io.digdag.client.api.RestProjectCollection;
import io.digdag.client.api.RestRevisionCollection;
import io.digdag.client.api.RestScheduleCollection;
import io.digdag.client.api.RestSecret;
import io.digdag.client.api.RestSecretList;
import io.digdag.client.api.RestSecretMetadata;
import io.digdag.client.api.RestSessionCollection;
import io.digdag.client.api.RestSetSecretRequest;
import io.digdag.client.api.RestWorkflowDefinition;
import io.digdag.client.api.RestWorkflowDefinitionCollection;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.TempFileManager;
import io.digdag.core.TempFileManager.TempDir;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import io.digdag.core.archive.WorkflowResourceMatcher;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ArchiveType;
import io.digdag.core.repository.ProjectControl;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.Revision;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredProjectWithRevision;
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
import io.digdag.metrics.DigdagTimed;
import io.digdag.server.GenericJsonExceptionHandler;
import io.digdag.server.rs.project.ProjectClearScheduleParam;
import io.digdag.server.rs.project.PutProjectsValidator;
import io.digdag.server.service.ProjectService;
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.SecretControlStore;
import io.digdag.spi.SecretControlStoreManager;
import io.digdag.spi.SecretScopes;
import io.digdag.client.api.SecretValidation;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.spi.StorageObject;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.ProjectContentTarget;
import io.digdag.spi.ac.ProjectTarget;
import io.digdag.spi.ac.SecretTarget;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;
import io.digdag.spi.metrics.DigdagMetrics;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    // GET  /api/projects/{id}/revisions?name=<name>     # lookup a revision of a project by name, or return an empty array
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
    private static int MAX_SESSIONS_PAGE_SIZE;
    private static final int DEFAULT_SESSIONS_PAGE_SIZE = 100;
    private final ArchiveManager archiveManager;
    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final AccessController ac;
    private final SessionStoreManager ssm;
    private final SecretControlStoreManager scsp;
    private final TransactionManager tm;
    private final DigdagMetrics metrics;
    private final ProjectService projectService;

    @Inject
    public ProjectResource(
            ArchiveManager archiveManager,
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            AccessController ac,
            SessionStoreManager ssm,
            SecretControlStoreManager scsp,
            TransactionManager tm,
            Config systemConfig,
            DigdagMetrics metrics,
            ProjectService projectService)
    {
        this.archiveManager = archiveManager;
        this.rm = rm;
        this.sm = sm;
        this.ac = ac;
        this.ssm = ssm;
        this.tm = tm;
        this.scsp = scsp;
        this.metrics = metrics;
        this.projectService = projectService;
        MAX_SESSIONS_PAGE_SIZE = systemConfig.get("api.max_sessions_page_size", Integer.class, DEFAULT_SESSIONS_PAGE_SIZE);
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

    @DigdagTimed(category = "api", appendMethodName = true)
    // /<singular> style is deprecated. Use /api/projects with filter instead
    @Deprecated
    @GET
    @Path("/api/project")
    @ApiOperation("(deprecated)")
    public RestProject getProject(@QueryParam("name") String name)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestProject, ResourceNotFoundException, AccessControlException>begin(() -> {
            Preconditions.checkArgument(name != null, "name= is required");

            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectByName(name)); // check NotFound first
            StoredRevision rev = ps.getLatestRevision(proj.getId()); // check NotFound first

            ac.checkGetProject( // AccessControl
                    ProjectTarget.of(getSiteId(), proj.getName(), proj.getId()),
                    getAuthenticatedUser());

            return RestModels.project(proj, rev);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/projects")
    @ApiOperation("List projects with filters")
    public RestProjectCollection getProjects(
            @ApiParam(value="exact matching filter on project name", required=false)
            @QueryParam("name") String name,
            @ApiParam(value="list projects whose id is grater than this id for pagination", required=false)
            @QueryParam("last_id") Integer lastId,
            @ApiParam(value="number of projects to return", required=false)
            @QueryParam("count") Integer count,
            @ApiParam(value="name pattern to be partially matched", required=false)
            @QueryParam("name_pattern") String namePattern)
    {
        return tm.begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());

            List<RestProject> collection;
            if (name != null) {
                try {
                    StoredProject proj = ensureNotDeletedProject(ps.getProjectByName(name)); // check NotFound first
                    StoredRevision rev = ps.getLatestRevision(proj.getId()); // check NotFound first

                    ac.checkGetProject( // AccessControl
                            ProjectTarget.of(getSiteId(), proj.getName(), proj.getId()),
                            getAuthenticatedUser());

                    collection = ImmutableList.of(RestModels.project(proj, rev));
                }
                catch (ResourceNotFoundException | AccessControlException ex) {
                    // Returning empty results or error should be consistent between AccessControl and NotFound.
                    // If it can return NotFound, it can return Forbidden. Otherwise, empty list only.
                    collection = ImmutableList.of();
                }
            }
            else {
                final SiteTarget siteTarget = SiteTarget.of(getSiteId());

                try {
                    ac.checkListProjectsOfSite( // AccessControl
                            siteTarget,
                            getAuthenticatedUser());

                    collection = ps.getProjectsWithLatestRevision(
                                Optional.fromNullable(count).or(100),
                                Optional.fromNullable(lastId),
                                Optional.fromNullable(namePattern),
                                ac.getListProjectsFilterOfSite(siteTarget, getAuthenticatedUser()))
                            .stream()
                            .map(projWithRev -> {
                                return RestModels.project(projWithRev);
                            })
                            .collect(Collectors.toList());
                }
                catch (AccessControlException ex) {
                    // Returning empty results or error should be consistent between AccessControl and NotFound.
                    // If it can return NotFound, it can return Forbidden. Otherwise, empty list only.
                    collection =  ImmutableList.of();
                }
            }

            return RestModels.projectCollection(collection);
        });
    }

    @DigdagTimed(category = "api", value = "getProjectById")
    @GET
    @Path("/api/projects/{id}")
    @ApiOperation("Get a project")
    public RestProject getProject(
            @ApiParam(value="project id", required=false)
            @PathParam("id") int projId)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestProject, ResourceNotFoundException, AccessControlException>begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId)); // check NotFound first
            StoredRevision rev = ps.getLatestRevision(proj.getId()); // check NotFound first

            ac.checkGetProject( // AccessControl
                    ProjectTarget.of(getSiteId(), proj.getName(), proj.getId()),
                    getAuthenticatedUser());

            return RestModels.project(proj, rev);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/projects/{id}/revisions")
    @ApiOperation("List revisions of a project")
    public RestRevisionCollection getRevisions(
            @PathParam("id") int projId,
            // last_id exists but impossible to use for pagination purpose because
            // RestRevision doesn't return id. This is a REST API design bug. No
            // clients can use this parameter appropriately.
            @ApiParam(value="deprecated - do not use")
            @QueryParam("last_id") Integer lastId,
            @ApiParam(value="revision name")
            @QueryParam("name") String revName)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestRevisionCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId)); // check NotFound first
            List<StoredRevision> revs;
            if (revName != null) {
                try {
                    StoredRevision rev = ps.getRevisionByName(proj.getId(), revName); // check NotFound first
                    revs = ImmutableList.of(rev);
                }
                catch (ResourceNotFoundException ex) {
                    revs = ImmutableList.of();
                }
            }
            else {
                revs = ps.getRevisions(proj.getId(), 100, Optional.fromNullable(lastId));
            }

            ac.checkGetProject( // AccessControl
                    ProjectTarget.of(getSiteId(), proj.getName(), proj.getId()),
                    getAuthenticatedUser());

            return RestModels.revisionCollection(proj, revs);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    // /<singular> style is deprecated. Use /api/projects/{id}/workflows with filter instead
    @Deprecated
    @GET
    @Path("/api/projects/{id}/workflow")
    @ApiOperation("(deprecated)")
    public RestWorkflowDefinition getWorkflow(@PathParam("id") int projId, @QueryParam("name") String name, @QueryParam("revision") String revName)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestWorkflowDefinition, ResourceNotFoundException, AccessControlException>begin(() -> {
            Preconditions.checkArgument(name != null, "name= is required");

            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId)); // check NotFound first

            StoredRevision rev;
            if (revName == null) {
                rev = ps.getLatestRevision(proj.getId()); // check NotFound first
            }
            else {
                rev = ps.getRevisionByName(proj.getId(), revName); // check NotFound first
            }
            StoredWorkflowDefinition def = ps.getWorkflowDefinitionByName(rev.getId(), name); // check NotFound first

            ac.checkGetWorkflow( // AccessControl
                    WorkflowTarget.of(getSiteId(), name, proj.getName()),
                    getAuthenticatedUser());

            return RestModels.workflowDefinition(proj, rev, def);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/projects/{id}/workflows/{name}")
    public RestWorkflowDefinition getWorkflowByName(@PathParam("id") int projId, @PathParam("name") String name, @QueryParam("revision") String revName)
            throws ResourceNotFoundException, AccessControlException
    {
        return getWorkflow(projId, name, revName);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/projects/{id}/workflows")
    @ApiOperation("List workflows of a project with filters")
    public RestWorkflowDefinitionCollection getWorkflows(
            @ApiParam(value="project id", required=true)
            @PathParam("id") int projId,
            @ApiParam(value="use a given revision of the project instead of the latest revision", required=true)
            @QueryParam("revision") String revName,
            @ApiParam(value="exact matching filter on workflow name", required=false)
            @QueryParam("name") String name)
            throws ResourceNotFoundException
    {
        return tm.<RestWorkflowDefinitionCollection, ResourceNotFoundException>begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId)); // check NotFound first

            StoredRevision rev;
            if (revName == null) {
                rev = ps.getLatestRevision(proj.getId()); // check NotFound first
            }
            else {
                rev = ps.getRevisionByName(proj.getId(), revName); // check NotFound first
            }

            List<StoredWorkflowDefinition> collection;
            if (name != null) {
                try {
                    StoredWorkflowDefinition def = ps.getWorkflowDefinitionByName(rev.getId(), name); // check NotFound first

                    ac.checkGetWorkflow( // AccessControl
                            WorkflowTarget.of(getSiteId(), def.getName(), proj.getName()),
                            getAuthenticatedUser());

                    collection = ImmutableList.of(def);
                }
                catch (ResourceNotFoundException | AccessControlException ex) {
                    // Returning empty results or error should be consistent between AccessControl and NotFound.
                    // If it can return NotFound, it can return Forbidden. Otherwise, empty list only.
                    collection = ImmutableList.of();
                }
            }
            else {
                // of project
                final ProjectTarget projTarget = ProjectTarget.of(getSiteId(), proj.getName(), proj.getId());

                try {
                    ac.checkListWorkflowsOfProject( // AccessControl
                            projTarget,
                            getAuthenticatedUser());

                    // TODO should here support pagination?
                    collection = ps.getWorkflowDefinitions(rev.getId(), Integer.MAX_VALUE, Optional.absent(),
                            ac.getListWorkflowsFilterOfProject(
                                    projTarget,
                                    getAuthenticatedUser()));
                }
                catch (AccessControlException ex) {
                    // Returning empty results or error should be consistent between AccessControl and NotFound.
                    // If it can return NotFound, it can return Forbidden. Otherwise, empty list only.
                    collection = ImmutableList.of();
                }
            }
            return RestModels.workflowDefinitionCollection(proj, rev, collection);
        }, ResourceNotFoundException.class);
    }

    @DigdagTimed(category = "api", value = "getProjectSchedules")
    @GET
    @ApiOperation("List schedules of a project with filters")
    @Path("/api/projects/{id}/schedules")
    public RestScheduleCollection getSchedules(
            @ApiParam(value="project id", required=true)
            @PathParam("id") int projectId,
            @ApiParam(value="exact matching filter on workflow name", required=false)
            @QueryParam("workflow") String workflowName,
            @ApiParam(value="list schedules whose id is grater than this id for pagination", required=false)
            @QueryParam("last_id") Integer lastId)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestScheduleCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            ProjectStore projectStore = rm.getProjectStore(getSiteId());
            ScheduleStore scheduleStore = sm.getScheduleStore(getSiteId());

            StoredProject proj = ensureNotDeletedProject(projectStore.getProjectById(projectId)); // check NotFound first

            List<StoredSchedule> scheds;
            if (workflowName != null) {
                // of workflow

                StoredSchedule sched;
                try {
                    ac.checkGetScheduleFromWorkflow( // AccessControl
                            WorkflowTarget.of(getSiteId(), workflowName, proj.getName()),
                            getAuthenticatedUser());

                    sched = scheduleStore.getScheduleByProjectIdAndWorkflowName(projectId, workflowName);
                    scheds = ImmutableList.of(sched);
                }
                catch (ResourceNotFoundException | AccessControlException ex) {
                    // Returning empty results or error should be consistent between AccessControl and NotFound.
                    // If it can return NotFound, it can return Forbidden. Otherwise, empty list only.
                    scheds = ImmutableList.of();
                }
            }
            else {
                // of project

                final ProjectTarget projTarget = ProjectTarget.of(getSiteId(), proj.getName(), proj.getId());
                ac.checkListSchedulesOfProject( // AccessControl
                        projTarget,
                        getAuthenticatedUser());

                scheds = scheduleStore.getSchedulesByProjectId(projectId, 100, Optional.fromNullable(lastId),
                        ac.getListSchedulesFilterOfProject(
                                projTarget,
                                getAuthenticatedUser()));
            }

            return RestModels.scheduleCollection(projectStore, scheds);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", value = "getProjectSessions")
    @GET
    @ApiOperation("List sessions of a project with filters")
    @Path("/api/projects/{id}/sessions")
    public RestSessionCollection getSessions(
            @ApiParam(value="project id", required=true)
            @PathParam("id") int projectId,
            @ApiParam(value="exact matching filter on workflow name", required=false)
            @QueryParam("workflow") String workflowName,
            @ApiParam(value="list sessions whose id is grater than this id for pagination", required=false)
            @QueryParam("last_id") Long lastId,
            @ApiParam(value="number of sessions to return", required=false)
            @QueryParam("page_size") Integer pageSize)
            throws ResourceNotFoundException, AccessControlException
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_SESSIONS_PAGE_SIZE, DEFAULT_SESSIONS_PAGE_SIZE);

        return tm.<RestSessionCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            SessionStore ss = ssm.getSessionStore(getSiteId());

            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projectId)); // check NotFound first

            List<StoredSessionWithLastAttempt> sessions;
            if (workflowName != null) {
                // of workflow

                final WorkflowTarget wfTarget = WorkflowTarget.of(getSiteId(), workflowName, proj.getName());
                ac.checkListSessionsOfWorkflow( // AccessControl
                        wfTarget,
                        getAuthenticatedUser());

                sessions = ss.getSessionsOfWorkflowByName(proj.getId(), workflowName, validPageSize, Optional.fromNullable(lastId),
                        ac.getListSessionsFilterOfWorkflow(
                                wfTarget,
                                getAuthenticatedUser()));
            }
            else {
                // of project

                final ProjectTarget projTarget = ProjectTarget.of(getSiteId(), proj.getName(), proj.getId());
                ac.checkListSessionsOfProject( // AccessControl
                        projTarget,
                        getAuthenticatedUser());

                sessions = ss.getSessionsOfProject(proj.getId(), validPageSize, Optional.fromNullable(lastId),
                        ac.getListSessionsFilterOfProject(
                                projTarget,
                                getAuthenticatedUser()));
            }

            return RestModels.sessionCollection(ps, sessions);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/projects/{id}/archive")
    @Produces("application/gzip")
    @ApiOperation("Download a project archive file")
    public Response getArchive(
            @ApiParam(value="project id", required=true)
            @PathParam("id") int projId,
            @ApiParam(value="use a given revision of a project instead of the latest revision", required=true)
            @QueryParam("revision") String revName,
            @ApiParam(value="enable returning direct download handle", required=false)
            @QueryParam("direct_download") Boolean directDownloadAllowed)
            throws ResourceNotFoundException, AccessControlException
    {
        // Disable direct download (redirection to direct download URL by returning
        // 303 See Other) if ?direct_download=false is given.
        boolean enableDirectDownload = (directDownloadAllowed == null) || (boolean) directDownloadAllowed;

        return tm.<Response, ResourceNotFoundException, AccessControlException>begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject proj = ensureNotDeletedProject(ps.getProjectById(projId)); // check NotFound first

            Optional<ArchiveManager.StoredArchive> archiveOrNone =
                    archiveManager.getArchive(ps, projId, revName); // check NotFound first

            ac.checkGetProjectArchive( // AccessControl
                    ProjectTarget.of(getSiteId(), proj.getName(), proj.getId()),
                    getAuthenticatedUser());

            if (!archiveOrNone.isPresent()) {
                throw new ResourceNotFoundException("Archive is not stored");
            }
            else {
                ArchiveManager.StoredArchive archive = archiveOrNone.get();

                if (enableDirectDownload) {
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
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @DELETE
    @Path("/api/projects/{id}")
    @ApiOperation("Delete a project")
    public RestProject deleteProject(@PathParam("id") int projId)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestProject, ResourceNotFoundException, AccessControlException>begin(() -> {
            ProjectStore ps = rm.getProjectStore(getSiteId());
            StoredProject project = ensureNotDeletedProject(ps.getProjectById(projId)); // check NotFound first

            ac.checkDeleteProject( // AccessControl
                    ProjectTarget.of(getSiteId(), project.getName(), project.getId()),
                    getAuthenticatedUser());

            return ProjectControl.deleteProject(ps, projId, (control, proj) -> {
                StoredRevision rev = ps.getLatestRevision(proj.getId()); // check NotFound first
                return RestModels.project(proj, rev);
            });
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @PUT
    @Consumes("application/gzip")
    @Path("/api/projects")
    @ApiOperation("Upload a project archive as a new project or a new revision of an existing project")
    public RestProject putProject(
            @ApiParam(value = "project name", required = true)
            @QueryParam("project") String name,
            @ApiParam(value = "revision", required = true)
            @QueryParam("revision") String revision,
            InputStream body,
            @HeaderParam("Content-Length") long contentLength,
            @ApiParam(value = "start scheduling of new workflows from the given time instead of current time", required = false)
            @QueryParam("schedule_from") String scheduleFromString,
            @ApiParam(value = "clear_schedule", required = false)
            @QueryParam("clear_schedule") List<String> clearSchedules,
            @ApiParam(value = "clear_schedule_all", required = false)
            @DefaultValue("false") @QueryParam("clear_schedule_all") boolean clearAllSchedules)
            throws ResourceConflictException, IOException, ResourceNotFoundException, AccessControlException
    {
        return projectService.putProject(
                getSiteId(),
                getUserInfo(),
                getAuthenticatedUser(),
                getSecrets(),
                name,
                revision,
                body,
                contentLength,
                scheduleFromString,
                clearSchedules,
                clearAllSchedules,
                Optional.absent());
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @PUT
    @Consumes("application/json")
    @Path("/api/projects/{id}/secrets/{key}")
    @ApiOperation("Set a secret to a project")
    public RestSecret putProjectSecret(
            @ApiParam(value="project id", required=true)
            @PathParam("id") int projectId,
            @ApiParam(value="secret key", required=true)
            @PathParam("key") String key,
            RestSetSecretRequest request)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestSecret, ResourceNotFoundException, AccessControlException>begin(() -> {
            if (!SecretValidation.isValidSecret(key, request.value())) {
                throw new IllegalArgumentException("Invalid secret");
            }

            // Verify that the project exists
            ProjectStore projectStore = rm.getProjectStore(getSiteId());
            StoredProject project = projectStore.getProjectById(projectId); // check NotFound first
            ensureNotDeletedProject(project);

            ac.checkPutProjectSecret( // AccessControl
                    SecretTarget.of(getSiteId(), key, project.getId(), project.getName()),
                    getAuthenticatedUser());

            SecretControlStore store = scsp.getSecretControlStore(getSiteId());

            store.setProjectSecret(projectId, SecretScopes.PROJECT, key, request.value());
            return RestModels.secret(project, key);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @DELETE
    @Path("/api/projects/{id}/secrets/{key}")
    @ApiOperation("Delete a secret from a project")
    public RestSecret deleteProjectSecret(
            @ApiParam(value="project id", required=true)
            @PathParam("id") int projectId,
            @ApiParam(value="secret key", required=true)
            @PathParam("key") String key)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestSecret, ResourceNotFoundException, AccessControlException>begin(() -> {
            if (!SecretValidation.isValidSecretKey(key)) {
                throw new IllegalArgumentException("Invalid secret");
            }

            // Verify that the project exists
            ProjectStore projectStore = rm.getProjectStore(getSiteId());
            StoredProject project = projectStore.getProjectById(projectId); // check NotFound first
            ensureNotDeletedProject(project);

            ac.checkDeleteProjectSecret( // AccessControl
                    SecretTarget.of(getSiteId(), key, project.getId(), project.getName()),
                    getAuthenticatedUser());

            SecretControlStore store = scsp.getSecretControlStore(getSiteId());

            store.deleteProjectSecret(projectId, SecretScopes.PROJECT, key);
            return RestModels.secret(project, key);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/projects/{id}/secrets")
    @Produces("application/json")
    @ApiOperation("List secret keys of a project")
    public RestSecretList getProjectSecrets(
            @ApiParam(value="project id", required=true)
            @PathParam("id") int projectId)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestSecretList, ResourceNotFoundException, AccessControlException>begin(() -> {
            // Verify that the project exists
            ProjectStore projectStore = rm.getProjectStore(getSiteId());
            StoredProject project = projectStore.getProjectById(projectId); // check NotFound first
            ensureNotDeletedProject(project);

            ac.checkGetProjectSecretList( // AccessControl
                    ProjectTarget.of(getSiteId(), project.getName(), project.getId()),
                    getAuthenticatedUser());

            SecretControlStore store = scsp.getSecretControlStore(getSiteId());
            List<String> keys = store.listProjectSecrets(projectId, SecretScopes.PROJECT);

            return RestSecretList.builder()
                    .secrets(keys.stream().map(RestSecretMetadata::of).collect(Collectors.toList()))
                    .build();
        }, ResourceNotFoundException.class, AccessControlException.class);
    }
}
