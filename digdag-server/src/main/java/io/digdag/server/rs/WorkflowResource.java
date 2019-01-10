package io.digdag.server.rs;

import java.util.List;
import java.util.function.Supplier;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.GET;

import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.spi.Scheduler;
import io.digdag.client.api.*;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;
import io.swagger.annotations.Api;

@Api("Workflow")
@Path("/")
@Produces("application/json")
public class WorkflowResource
    extends AuthenticatedResource
{
    // GET  /api/workflows                                   # list workflows of the latest revisions of active projects
    // GET  /api/workflows/{id}                              # get a workflow
    // GET  /api/workflows/{id}/truncated_session_time       # truncate a time based on timzeone of this workflow
    //
    // Deprecated:
    // GET  /api/workflow?project=<name>&name=<name>      # lookup a workflow of the latest revision of a project by name
    // GET  /api/workflow?project=<name>&revision=<name>&name=<name>  # lookup a workflow of a past revision of a project by name

    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final SchedulerManager srm;
    private final TransactionManager tm;
    private final AccessController ac;

    @Inject
    public WorkflowResource(
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            SchedulerManager srm,
            TransactionManager tm,
            AccessController ac)
    {
        this.rm = rm;
        this.sm = sm;
        this.srm = srm;
        this.tm = tm;
        this.ac = ac;
    }

    @GET
    @Path("/api/workflow")
    public RestWorkflowDefinition getWorkflowDefinition(
            @QueryParam("project") String projName,
            @QueryParam("revision") String revName,
            @QueryParam("name") String wfName)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestWorkflowDefinition, ResourceNotFoundException, AccessControlException>begin(() -> {
            Preconditions.checkArgument(projName != null, "project= is required");
            Preconditions.checkArgument(wfName != null, "name= is required");

            ProjectStore rs = rm.getProjectStore(getSiteId());
            StoredProject proj = rs.getProjectByName(projName); // NotFound
            StoredRevision rev;

            if (revName == null) {
                rev = rs.getLatestRevision(proj.getId()); // NotFound
            }
            else {
                rev = rs.getRevisionByName(proj.getId(), revName); // NotFound
            }
            StoredWorkflowDefinition def = rs.getWorkflowDefinitionByName(rev.getId(), wfName); // NotFound

            ac.checkGetWorkflow( // AccessControl
                    WorkflowTarget.of(getSiteId(), def.getName(), proj.getName()),
                    getUserInfo());

            return RestModels.workflowDefinition(proj, rev, def);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @GET
    @Path("/api/workflows")
    public RestWorkflowDefinitionCollection getWorkflowDefinitions(
            @QueryParam("last_id") Long lastId,
            @QueryParam("count") Integer count)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestWorkflowDefinitionCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            List<StoredWorkflowDefinitionWithProject> defs =
                    rm.getProjectStore(getSiteId())
                            .getLatestActiveWorkflowDefinitions(Optional.fromNullable(count).or(100), Optional.fromNullable(lastId), // NotFound
                                    ac.getListWorkflowsFilterOfSite(
                                            SiteTarget.of(getSiteId()),
                                            getUserInfo()));

            // The operation is not permitted if getting some of workflows is not permitted.
            for (StoredWorkflowDefinitionWithProject def : defs) {
                ac.checkListWorkflows( // AccessControl
                        WorkflowTarget.of(getSiteId(), def.getName(), def.getProject().getName()),
                        getUserInfo());
            }

            return RestModels.workflowDefinitionCollection(defs);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @GET
    @Path("/api/workflows/{id}")
    public RestWorkflowDefinition getWorkflowDefinition(@PathParam("id") long id)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestWorkflowDefinition, ResourceNotFoundException, AccessControlException>begin(() -> {
            StoredWorkflowDefinitionWithProject def =
                    rm.getProjectStore(getSiteId())
                            .getWorkflowDefinitionById(id); // NotFound

            ac.checkGetWorkflow( // AccessControl
                    WorkflowTarget.of(getSiteId(), def.getName(), def.getProject().getName()),
                    getUserInfo());

            return RestModels.workflowDefinition(def);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @GET
    @Path("/api/workflows/{id}/truncated_session_time")
    public RestWorkflowSessionTime getWorkflowDefinition(
            @PathParam("id") long id,
            @QueryParam("session_time") LocalTimeOrInstant localTime,
            @QueryParam("mode") SessionTimeTruncate mode)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestWorkflowSessionTime, ResourceNotFoundException, AccessControlException>begin(() -> {
            Preconditions.checkArgument(localTime != null, "session_time= is required");

            StoredWorkflowDefinitionWithProject def =
                    rm.getProjectStore(getSiteId())
                            .getWorkflowDefinitionById(id); // NotFound

            ac.checkGetWorkflow( // AccessControl
                    WorkflowTarget.of(getSiteId(), def.getName(), def.getProject().getName()),
                    getUserInfo());

            ZoneId timeZone = def.getTimeZone();

            Instant truncated;
            if (mode != null) {
                truncated = truncateSessionTime(
                        localTime.toInstant(timeZone),
                        timeZone, () -> srm.tryGetScheduler(def), mode);
            }
            else {
                truncated = localTime.toInstant(timeZone);
            }

            return RestModels.workflowSessionTime(
                    def, truncated, timeZone);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    private Instant truncateSessionTime(
            Instant sessionTime,
            ZoneId timeZone,
            Supplier<Optional<Scheduler>> schedulerSupplier,
            SessionTimeTruncate mode)
    {
        switch (mode) {
        case HOUR:
            return ZonedDateTime.ofInstant(sessionTime, timeZone)
                .truncatedTo(ChronoUnit.HOURS)
                .toInstant();
        case DAY:
            return ZonedDateTime.ofInstant(sessionTime, timeZone)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant();
        default:
            {
                Optional<Scheduler> scheduler = schedulerSupplier.get();
                if (!scheduler.isPresent()) {
                    throw new IllegalArgumentException("session_time_truncate=" + mode + " is set but schedule is not set to this workflow");
                }
                switch (mode) {
                case SCHEDULE:
                    return scheduler.get().getFirstScheduleTime(sessionTime).getTime();
                case NEXT_SCHEDULE:
                    return scheduler.get().nextScheduleTime(sessionTime).getTime();
                default:
                    throw new IllegalArgumentException();
                }
            }
        }
    }
}
