package io.digdag.server.rs;

import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.stream.Collectors;
import javax.ws.rs.Consumes;
import javax.ws.rs.NotFoundException;
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
import io.digdag.spi.ScheduleTime;

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
    private final AttemptBuilder attemptBuilder;
    private final WorkflowExecutor executor;
    private final ConfigFactory cf;

    @Inject
    public AttemptResource(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            SchedulerManager srm,
            TransactionManager tm,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor executor,
            ConfigFactory cf)
    {
        this.rm = rm;
        this.sm = sm;
        this.srm = srm;
        this.tm = tm;
        this.attemptBuilder = attemptBuilder;
        this.executor = executor;
        this.cf = cf;
    }

    @GET
    @Path("/api/attempts")
    public RestSessionAttemptCollection getAttempts(
            @QueryParam("project") String projName,
            @QueryParam("workflow") String wfName,
            @QueryParam("include_retried") boolean includeRetried,
            @QueryParam("last_id") Long lastId)
            throws Exception
    {
        return tm.begin(() -> {
            List<StoredSessionAttemptWithSession> attempts;

            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());
            if (projName != null) {
                StoredProject proj = rs.getProjectByName(projName);
                if (wfName != null) {
                    // of workflow
                    StoredWorkflowDefinition wf = rs.getLatestWorkflowDefinitionByName(proj.getId(), wfName);
                    attempts = ss.getAttemptsOfWorkflow(includeRetried, wf.getId(), 100, Optional.fromNullable(lastId));
                }
                else {
                    // of project
                    attempts = ss.getAttemptsOfProject(includeRetried, proj.getId(), 100, Optional.fromNullable(lastId));
                }
            }
            else {
                // of site
                attempts = ss.getAttempts(includeRetried, 100, Optional.fromNullable(lastId));
            }

            return RestModels.attemptCollection(rm.getProjectStore(getSiteId()), attempts);
        });
    }

    @GET
    @Path("/api/attempts/{id}")
    public RestSessionAttempt getAttempt(@PathParam("id") long id)
            throws Exception
    {
        return tm.begin(() -> {
            StoredSessionAttemptWithSession attempt = sm.getSessionStore(getSiteId())
                    .getAttemptById(id);
            StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(attempt.getSession().getProjectId());

            return RestModels.attempt(attempt, proj.getName());
        });
    }

    @GET
    @Path("/api/attempts/{id}/retries")
    public RestSessionAttemptCollection getAttemptRetries(@PathParam("id") long id)
            throws Exception
    {
        return tm.begin(() -> {
            List<StoredSessionAttemptWithSession> attempts = sm.getSessionStore(getSiteId())
                    .getOtherAttempts(id);

            return RestModels.attemptCollection(rm.getProjectStore(getSiteId()), attempts);
        });
    }

    @GET
    @Path("/api/attempts/{id}/tasks")
    public RestTaskCollection getTasks(@PathParam("id") long id)
            throws Exception
    {
        return tm.begin(() -> {
            List<ArchivedTask> tasks = sm.getSessionStore(getSiteId())
                    .getTasksOfAttempt(id);
            return RestModels.taskCollection(tasks);
        });
    }

    @PUT
    @Consumes("application/json")
    @Path("/api/attempts")
    public Response startAttempt(RestSessionAttemptRequest request)
            throws Exception
    {
        return tm.begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());

            StoredWorkflowDefinitionWithProject def = rs.getWorkflowDefinitionById(
                    RestModels.parseWorkflowId(request.getWorkflowId()));

            Optional<Long> resumingAttemptId = request.getResume()
                    .transform(r -> RestModels.parseAttemptId(r.getAttemptId()));
            List<Long> resumingTasks = request.getResume()
                    .transform(r -> collectResumingTasks(r))
                    .or(ImmutableList.of());

            // use the HTTP request time as the runTime
            AttemptRequest ar = attemptBuilder.buildFromStoredWorkflow(
                    def,
                    request.getParams(),
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
        });
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

    @POST
    @Consumes("application/json")
    @Path("/api/attempts/{id}/kill")
    public void killAttempt(@PathParam("id") long id)
            throws Exception
    {
        tm.begin(() -> {
            boolean updated = executor.killAttemptById(getSiteId(), id);
            if (!updated) {
                throw new ResourceConflictException("Session attempt already killed or finished");
            }
            return null;
        });
    }
}
