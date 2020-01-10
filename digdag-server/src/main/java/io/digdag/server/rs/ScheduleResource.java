package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleAttemptCollection;
import io.digdag.client.api.RestScheduleCollection;
import io.digdag.client.api.RestScheduleBackfillRequest;
import io.digdag.client.api.RestScheduleSkipRequest;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSessionAttemptCollection;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.schedule.ScheduleControl;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.ScheduleTarget;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;
import io.digdag.spi.metrics.DigdagMetrics;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import java.time.ZoneId;
import java.util.List;

@Api("Schedule")
@Path("/")
@Produces("application/json")
public class ScheduleResource
    extends AuthenticatedResource
{
    // GET  /api/schedules                                   # list schedules of the latest revision of all projects
    // GET  /api/schedules/{id}                              # show a particular schedule (which belongs to a workflow)
    // POST /api/schedules/{id}/skip                         # skips schedules forward to a future time
    // POST /api/schedules/{id}/backfill                     # run or re-run past schedules
    // POST /api/schedules/{id}/disable                      # disable a schedule
    // POST /api/schedules/{id}/enable                       # enable a schedule

    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final TransactionManager tm;
    private final AccessController ac;
    private final ScheduleExecutor exec;
    private final DigdagMetrics metrics;

    @Inject
    public ScheduleResource(
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            TransactionManager tm,
            AccessController ac,
            ScheduleExecutor exec,
            DigdagMetrics metrics)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        this.ac = ac;
        this.exec = exec;
        this.metrics = metrics;
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @GET
    @Path("/api/schedules")
    @ApiOperation("List schedules")
    public RestScheduleCollection getSchedules(
            @ApiParam(value="list schedules whose id is grater than this id for pagination", required=false)
            @QueryParam("last_id") Integer lastId)
            throws AccessControlException
    {
        final SiteTarget siteTarget = SiteTarget.of(getSiteId());
        ac.checkListSchedulesOfSite( // AccessControl
                siteTarget,
                getAuthenticatedUser());

        return tm.begin(() -> {
            List<StoredSchedule> scheds = sm.getScheduleStore(getSiteId())
                    .getSchedules(100, Optional.fromNullable(lastId),
                            ac.getListSchedulesFilterOfSite(
                                    siteTarget,
                                    getAuthenticatedUser()));

            return RestModels.scheduleCollection(rm.getProjectStore(getSiteId()), scheds);
        });
    }

    @DigdagTimed(category = "api", value = "getScheduleById")
    @GET
    @Path("/api/schedules/{id}")
    @ApiOperation("Get a schedule")
    public RestSchedule getSchedules(
            @ApiParam(value="schedule id", required=true)
            @PathParam("id") int id)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestSchedule, ResourceNotFoundException, AccessControlException>begin(() -> {
            StoredSchedule sched = sm.getScheduleStore(getSiteId())
                    .getScheduleById(id); // check NotFound first
            ZoneId timeZone = getTimeZoneOfSchedule(sched);

            StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(sched.getProjectId()); // check NotFound first

            ac.checkGetSchedule( // AccessControl
                    WorkflowTarget.of(getSiteId(), sched.getWorkflowName(), proj.getName()),
                    getAuthenticatedUser());

            return RestModels.schedule(sched, proj, timeZone);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @POST
    @Consumes("application/json")
    @Path("/api/schedules/{id}/skip")
    @ApiOperation("Skip future sessions by count or time")
    public RestScheduleSummary skipSchedule(
            @ApiParam(value="session id", required=true)
            @PathParam("id") int id,
            RestScheduleSkipRequest request)
            throws ResourceConflictException, ResourceNotFoundException, AccessControlException
    {
        return tm.<RestScheduleSummary, ResourceConflictException, ResourceNotFoundException, AccessControlException>begin(() -> {
            Preconditions.checkArgument(request.getNextTime().isPresent() ||
                    (request.getCount().isPresent() && request.getFromTime().isPresent()), "nextTime or (fromTime and count) are required");

            StoredSchedule sched = sm.getScheduleStore(getSiteId())
                    .getScheduleById(id); // check NotFound first
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(sched.getProjectId()); // check NotFound first
            ZoneId timeZone = getTimeZoneOfSchedule(sched);

            ac.checkSkipSchedule( // AccessControl
                    ScheduleTarget.of(getSiteId(), proj.getName(), sched.getWorkflowName(), sched.getId()),
                    getAuthenticatedUser());

            StoredSchedule updated;
            if (request.getNextTime().isPresent()) {
                updated = exec.skipScheduleToTime(getSiteId(), id,
                        request.getNextTime().get().toInstant(timeZone),
                        request.getNextRunTime(),
                        request.getDryRun());
            }
            else {
                updated = exec.skipScheduleByCount(getSiteId(), id,
                        request.getFromTime().get(),
                        request.getCount().get(),
                        request.getNextRunTime(),
                        request.getDryRun());
            }

            return RestModels.scheduleSummary(updated, proj, timeZone);
        }, ResourceConflictException.class, ResourceNotFoundException.class, AccessControlException.class);
    }

    private ZoneId getTimeZoneOfSchedule(StoredSchedule sched)
            throws ResourceNotFoundException
    {
        // TODO optimize
        return rm.getProjectStore(getSiteId())
                .getWorkflowDefinitionById(sched.getWorkflowDefinitionId()) // NotFound
                .getTimeZone();
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @POST
    @Consumes("application/json")
    @Path("/api/schedules/{id}/backfill")
    @ApiOperation("Re-schedule past sessions by count or duration")
    public RestScheduleAttemptCollection backfillSchedule(
            @ApiParam(value="session id", required=true)
            @PathParam("id") int id,
            RestScheduleBackfillRequest request)
            throws ResourceConflictException, ResourceLimitExceededException, ResourceNotFoundException, AccessControlException
    {
        return tm.<RestScheduleAttemptCollection, ResourceConflictException, ResourceLimitExceededException, ResourceNotFoundException, AccessControlException>begin(() ->
        {
            final StoredSchedule sched = sm.getScheduleStore(getSiteId())
                    .getScheduleById(id); // check NotFound first
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(sched.getProjectId()); // check NotFound first

            ac.checkBackfillSchedule( // AccessControl
                    ScheduleTarget.of(getSiteId(), proj.getName(), sched.getWorkflowName(), sched.getId()),
                    getAuthenticatedUser());

            List<StoredSessionAttemptWithSession> attempts =
                    exec.backfill(getSiteId(), id,  // should never throw NotFound
                            request.getFromTime(),
                            request.getAttemptName(),
                            request.getCount(),
                            request.getDryRun());

            return RestModels.attemptCollection(sched, proj, rm.getProjectStore(getSiteId()), attempts);
        }, ResourceConflictException.class, ResourceLimitExceededException.class, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @POST
    @Path("/api/schedules/{id}/disable")
    @ApiOperation("Disable scheduling of new sessions")
    public RestScheduleSummary disableSchedule(
            @ApiParam(value="session id", required=true)
            @PathParam("id") int id)
            throws ResourceNotFoundException, ResourceConflictException, AccessControlException
    {
        return tm.<RestScheduleSummary, ResourceConflictException, ResourceNotFoundException, AccessControlException>begin(() ->
        {
            // TODO: this is racy
            StoredSchedule sched = sm.getScheduleStore(getSiteId())
                    .getScheduleById(id); // check NotFound first
            ZoneId timeZone = getTimeZoneOfSchedule(sched);
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(sched.getProjectId()); // check NotFound first

            ac.checkDisableSchedule( // AccessControl
                    ScheduleTarget.of(getSiteId(), proj.getName(), sched.getWorkflowName(), sched.getId()),
                    getAuthenticatedUser());

            StoredSchedule updated = sm.getScheduleStore(getSiteId()).updateScheduleById(id, (store, storedSchedule) -> { // should never throw NotFound
                ScheduleControl lockedSched = new ScheduleControl(store, storedSchedule);
                lockedSched.disableSchedule();
                return lockedSched.get();
            });

            return RestModels.scheduleSummary(updated, proj, timeZone);
        }, ResourceConflictException.class, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(category = "api", appendMethodName = true)
    @POST
    @Path("/api/schedules/{id}/enable")
    @ApiOperation("Re-enable disabled scheduling")
    public RestScheduleSummary enableSchedule(
            @ApiParam(value="session id", required=true)
            @PathParam("id") int id)
            throws ResourceNotFoundException, ResourceConflictException, AccessControlException
    {
        return tm.<RestScheduleSummary, ResourceConflictException, ResourceNotFoundException, AccessControlException>begin(() -> {
            // TODO: this is racy
            StoredSchedule sched = sm.getScheduleStore(getSiteId())
                    .getScheduleById(id); // check NotFound first
            ZoneId timeZone = getTimeZoneOfSchedule(sched);
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(sched.getProjectId()); // check NotFound first

            ac.checkEnableSchedule( // AccessControl
                    ScheduleTarget.of(getSiteId(), proj.getName(), sched.getWorkflowName(), sched.getId()),
                    getAuthenticatedUser());

            StoredSchedule updated = sm.getScheduleStore(getSiteId()).updateScheduleById(id, (store, storedSchedule) -> { // should never throw NotFound
                ScheduleControl lockedSched = new ScheduleControl(store, storedSchedule);
                lockedSched.enableSchedule();
                return lockedSched.get();
            });

            return RestModels.scheduleSummary(updated, proj, timeZone);
        }, ResourceConflictException.class, ResourceNotFoundException.class, AccessControlException.class);
    }
}
