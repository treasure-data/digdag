package io.digdag.server.rs;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import javax.ws.rs.core.Response;

import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.TaskRelation;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.api.*;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.AuthenticatedUser;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.ac.AttemptTarget;
import io.digdag.spi.ac.ProjectTarget;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;
import io.digdag.spi.metrics.DigdagMetrics;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api("Attempt")
@Path("/")
@Produces("application/json")
public class AttemptResource
    extends AuthenticatedResource
{
    // GET  /api/attempts                                    # list attempts from recent to old
    // GET  /api/attempts?include_retried=1                  # list attempts from recent to old
    // GET  /api/attempts?project=<name>                     # list attempts that belong to a particular project
    // GET  /api/attempts?project=<name>&workflow=<name>     # list attempts that belong to a particular workflow
    // GET  /api/attempts/{id}                               # show a session
    // GET  /api/attempts/{id}/tasks                         # list tasks of a session
    // GET  /api/attempts/{id}/retries                       # list retried attempts of this session
    // PUT  /api/attempts                                    # starts a new session
    // POST /api/attempts/{id}/kill                          # kill a session

    private final ProjectStoreManager rm;
    private final SessionStoreManager sm;
    private final SchedulerManager srm;
    private final TransactionManager tm;
    private final AccessController ac;
    private final AttemptBuilder attemptBuilder;
    private final WorkflowExecutor executor;
    private final ConfigFactory cf;
    private static final int DEFAULT_ATTEMPTS_PAGE_SIZE = 100;
    private static int MAX_ATTEMPTS_PAGE_SIZE;
    private final DigdagMetrics metrics;

    @Inject
    public AttemptResource(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            SchedulerManager srm,
            TransactionManager tm,
            AccessController ac,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor executor,
            ConfigFactory cf,
            Config systemConfig,
            DigdagMetrics metrics)
    {
        this.rm = rm;
        this.sm = sm;
        this.srm = srm;
        this.tm = tm;
        this.ac = ac;
        this.attemptBuilder = attemptBuilder;
        this.executor = executor;
        this.cf = cf;
        this.metrics = metrics;
        MAX_ATTEMPTS_PAGE_SIZE = systemConfig.get("api.max_attempts_page_size", Integer.class, DEFAULT_ATTEMPTS_PAGE_SIZE);
    }


    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/attempts")
    @ApiOperation("List attempts with filters")
    public RestSessionAttemptCollection getAttempts(
            @ApiParam(value="exact matching filter on project name", required=false)
            @QueryParam("project") String projName,
            @ApiParam(value="exact matching filter on workflow name", required=false)
            @QueryParam("workflow") String wfName,
            @ApiParam(value="list more than 1 attempts per session", required=false)
            @QueryParam("include_retried") boolean includeRetried,
            @ApiParam(value="list attempts whose id is grater than this id for pagination", required=false)
            @QueryParam("last_id") Long lastId,
            @ApiParam(value="number of attempts to return", required=false)
            @QueryParam("page_size") Integer pageSize)
            throws ResourceNotFoundException, AccessControlException
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_ATTEMPTS_PAGE_SIZE, DEFAULT_ATTEMPTS_PAGE_SIZE);
        return tm.<RestSessionAttemptCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            List<StoredSessionAttemptWithSession> attempts;

            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            if (projName != null) {
                final StoredProject proj = rs.getProjectByName(projName); // check NotFound first
                if (wfName != null) {
                    // of workflow

                    final WorkflowTarget wfTarget = WorkflowTarget.of(getSiteId(), wfName, proj.getName());
                    ac.checkListSessionsOfWorkflow(wfTarget, getAuthenticatedUser()); // AccessControl
                    attempts = ss.getAttemptsOfWorkflow(includeRetried, proj.getId(), wfName, validPageSize, Optional.fromNullable(lastId),
                            ac.getListSessionsFilterOfWorkflow(
                                    wfTarget,
                                    getAuthenticatedUser()));
                }
                else {
                    // of project

                    final ProjectTarget projTarget = ProjectTarget.of(getSiteId(), projName, proj.getId());
                    ac.checkListSessionsOfProject(projTarget, getAuthenticatedUser()); // AccessControl
                    attempts = ss.getAttemptsOfProject(includeRetried, proj.getId(), validPageSize, Optional.fromNullable(lastId),
                            ac.getListSessionsFilterOfProject(
                                    projTarget,
                                    getAuthenticatedUser()));
                }
            }
            else {
                // of site

                final SiteTarget siteTarget = SiteTarget.of(getSiteId());
                ac.checkListSessionsOfSite(siteTarget, getAuthenticatedUser()); // AccessControl
                attempts = ss.getAttempts(includeRetried, validPageSize, Optional.fromNullable(lastId),
                        ac.getListSessionsFilterOfSite(
                                siteTarget,
                                getAuthenticatedUser()));
            }

            return RestModels.attemptCollection(rm.getProjectStore(getSiteId()), attempts);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category="api", appendMethodName = true)
    @GET
    @Path("/api/attempts/{id}")
    @ApiOperation("Get an attempt")
    public RestSessionAttempt getAttempt(
            @ApiParam(value="attempt id", required=true)
            @PathParam("id") long id)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestSessionAttempt, ResourceNotFoundException, AccessControlException >begin(() -> {
            final StoredSessionAttemptWithSession attempt = sm.getSessionStore(getSiteId())
                    .getAttemptById(id); // check NotFound first
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(attempt.getSession().getProjectId()); // to build WorkflowTarget

            ac.checkGetAttempt( // AccessControl
                    WorkflowTarget.of(getSiteId(),attempt.getSession().getWorkflowName(), proj.getName()),
                    getAuthenticatedUser());

            return RestModels.attempt(attempt, proj.getName());
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/attempts/{id}/retries")
    @ApiOperation("List attempts of a session of a given attempt")
    public RestSessionAttemptCollection getAttemptRetries(
            @ApiParam(value="attempt id", required=true)
            @PathParam("id") long id)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestSessionAttemptCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            final StoredSessionAttemptWithSession attempt = sm.getSessionStore(getSiteId())
                    .getAttemptById(id); // check NotFound first
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(attempt.getSession().getProjectId()); // to build WorkflowTarget

            ac.checkGetAttemptsFromSession( // AccessControl
                    WorkflowTarget.of(getSiteId(), attempt.getSession().getWorkflowName(), proj.getName()),
                    getAuthenticatedUser());

            List<StoredSessionAttemptWithSession> attempts = sm.getSessionStore(getSiteId())
                    .getOtherAttempts(id); // should never throw NotFound

            return RestModels.attemptCollection(rm.getProjectStore(getSiteId()), attempts);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/attempts/{id}/tasks")
    @ApiOperation("List tasks of an attempt")
    public RestTaskCollection getTasks(
            @ApiParam(value="attempt id", required=true)
            @PathParam("id") long id)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestTaskCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            final StoredSessionAttemptWithSession attempt = sm.getSessionStore(getSiteId())
                    .getAttemptById(id); // NotFound
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(attempt.getSession().getProjectId()); // NotFound

            ac.checkGetTasksFromAttempt( // AccessControl
                    WorkflowTarget.of(getSiteId(), attempt.getSession().getWorkflowName(), proj.getName()),
                    getAuthenticatedUser());

            List<ArchivedTask> tasks = sm.getSessionStore(getSiteId())
                    .getTasksOfAttempt(id);
            return RestModels.taskCollection(tasks);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @PUT
    @Consumes("application/json")
    @Path("/api/attempts")
    @ApiOperation("Start a workflow execution as a new session or a new attempt of an existing session")
    public Response startAttempt(RestSessionAttemptRequest request)
            throws AttemptLimitExceededException, TaskLimitExceededException, ResourceNotFoundException, AccessControlException
    {
        return tm.<Response, AttemptLimitExceededException, ResourceNotFoundException, TaskLimitExceededException, AccessControlException>begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            final StoredWorkflowDefinitionWithProject def = rs.getWorkflowDefinitionById( // check NotFound first
                    RestModels.parseWorkflowId(request.getWorkflowId()));

            AuthenticatedUser authedUser = getAuthenticatedUser();
            ac.checkRunWorkflow( // AccessControl
                    WorkflowTarget.of(getSiteId(), def.getName(), def.getProject().getName()),
                    authedUser);

            Optional<Long> resumingAttemptId = request.getResume()
                    .transform(r -> RestModels.parseAttemptId(r.getAttemptId()));
            List<Long> resumingTasks = request.getResume()
                    .transform(r -> collectResumingTasks(r))
                    .or(ImmutableList.of());

            Config params = request.getParams();
            processAuthenticatedUserInfo(authedUser, params);

            // use the HTTP request time as the runTime
            AttemptRequest ar = attemptBuilder.buildFromStoredWorkflow(
                    def,
                    params,
                    ScheduleTime.runNow(request.getSessionTime()),
                    request.getRetryAttemptName(),
                    resumingAttemptId,
                    resumingTasks,
                    Optional.absent());

            try {
                StoredSessionAttemptWithSession attempt = executor.submitWorkflow(getSiteId(), ar, def);
                RestSessionAttempt res = RestModels.attempt(attempt, def.getProject().getName());
                return Response.ok(res).build();
            }
            catch (SessionAttemptConflictException ex) {
                StoredSessionAttemptWithSession conflicted = ex.getConflictedSession();
                RestSessionAttempt res = RestModels.attempt(conflicted, def.getProject().getName());
                return Response.status(Response.Status.CONFLICT).entity(res).build();
            }
        }, AttemptLimitExceededException.class, ResourceNotFoundException.class, TaskLimitExceededException.class, AccessControlException.class);
    }

    protected void processAuthenticatedUserInfo(@Nonnull AuthenticatedUser user, @Nonnull Config params)
    {
        // nop by the default. Override if required
    }

    private List<Long> collectResumingTasks(RestSessionAttemptRequest.Resume resume)
    {
        switch (resume.getMode()) {
            case FAILED:
                return collectResumingTasksForResumeFailedMode(
                        RestModels.parseAttemptId(
                                ((RestSessionAttemptRequest.ResumeFailed) resume).getAttemptId()));
            case FROM:
                return collectResumingTasksForResumeFromMode(
                        RestModels.parseAttemptId(
                                ((RestSessionAttemptRequest.ResumeFrom) resume).getAttemptId()),
                        ((RestSessionAttemptRequest.ResumeFrom) resume).getFromTaskNamePattern());
            default:
                throw new IllegalArgumentException("Unknown resuming mode: " + resume.getMode());
        }
    }

    private List<Long> collectResumingTasksForResumeFailedMode(long attemptId)
    {
        List<ArchivedTask> tasks = sm
                .getSessionStore(getSiteId())
                .getTasksOfAttempt(attemptId);

        List<Long> successTasks = tasks.stream()
                .filter(task -> task.getState() == TaskStateCode.SUCCESS)
                .map(task -> {
                    if (!task.getParentId().isPresent()) {
                        throw new IllegalArgumentException("Resuming successfully completed attempts is not supported");
                    }
                    return task.getId();
                })
                .collect(Collectors.toList());

        return ImmutableList.copyOf(successTasks);
    }

    private List<Long> collectResumingTasksForResumeFromMode(long attemptId, String fromTaskPattern)
    {
        List<ArchivedTask> tasks = sm
                .getSessionStore(getSiteId())
                .getTasksOfAttempt(attemptId);

        ArchivedTask fromTask = matchTaskPattern(fromTaskPattern, tasks);

        List<TaskRelation> relations = tasks.stream()
                .map(t -> TaskRelation.of(t.getId(), t.getParentId(), t.getUpstreams()))
                .collect(Collectors.toList());
        TaskTree taskTree = new TaskTree(relations);

        List<Long> before = taskTree.getRecursiveParentsUpstreamChildrenIdListFromFar(fromTask.getId());
        List<Long> parents = taskTree.getRecursiveParentIdListFromRoot(fromTask.getId());

        Set<Long> results = new HashSet<>(before);
        results.removeAll(parents);

        return ImmutableList.copyOf(results);
    }

    private ArchivedTask matchTaskPattern(String pattern, List<ArchivedTask> tasks)
    {
        try {
            return TaskMatchPattern.compile(pattern).find(
                    tasks
                            .stream()
                            .collect(
                                    Collectors.toMap(t -> t.getFullName(), t -> t)
                            ));
        }
        catch (TaskMatchPattern.MultipleTaskMatchException | TaskMatchPattern.NoMatchException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @POST
    @Consumes("application/json")
    @Path("/api/attempts/{id}/kill")
    @ApiOperation("Set a cancel-requested flag on a running attempt")
    public Response killAttempt(
            @ApiParam(value="attempt id", required=true)
            @PathParam("id") long id)
            throws ResourceNotFoundException, ResourceConflictException, AccessControlException
    {
        return tm.<Response, ResourceNotFoundException, ResourceConflictException, AccessControlException>begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            final StoredSessionAttemptWithSession attempt = sm.getSessionStore(getSiteId())
                    .getAttemptById(id); // check NotFound first
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(attempt.getSession().getProjectId()); // to build WorkflowTarget
            final StoredWorkflowDefinitionWithProject def = rs.getWorkflowDefinitionById( attempt.getWorkflowDefinitionId().or(-1L));

            ac.checkKillAttempt( // AccessControl
                    AttemptTarget.of(getSiteId(), proj.getName(), attempt.getSession().getWorkflowName(), attempt.getSessionId(), attempt.getId()),
                    getAuthenticatedUser());

            boolean updated = executor.killAttemptById(getSiteId(), id); // should never throw NotFound
            if (!updated) {
                throw new ResourceConflictException("Session attempt already killed or finished");
            }
            RestSessionAttempt res = RestModels.attempt(attempt, def.getProject().getName());
            return Response.ok(res).build();
        }, ResourceNotFoundException.class, ResourceConflictException.class, AccessControlException.class);
    }
}
