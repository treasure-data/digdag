package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.api.RestSchedule;
import io.digdag.client.api.RestScheduleCollection;
import io.digdag.client.api.RestScheduleBackfillRequest;
import io.digdag.client.api.RestScheduleSkipRequest;
import io.digdag.client.api.RestScheduleSummary;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptCollection;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.schedule.ScheduleControl;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.schedule.ScheduleStore;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.core.session.StoredSessionAttemptWithSession;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

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
    private final ScheduleExecutor exec;

    @Inject
    public ScheduleResource(
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            TransactionManager tm,
            ScheduleExecutor exec)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        this.exec = exec;
    }

    @GET
    @Path("/api/schedules")
    public RestScheduleCollection getSchedules(@QueryParam("last_id") Integer lastId)
    {
        List<StoredSchedule> scheds = sm.getScheduleStore(getSiteId())
            .getSchedules(100, Optional.fromNullable(lastId));

        return RestModels.scheduleCollection(rm.getProjectStore(getSiteId()), scheds);
    }

    @GET
    @Path("/api/schedules/{id}")
    public RestSchedule getSchedules(@PathParam("id") int id)
        throws ResourceNotFoundException
    {
        StoredSchedule sched = sm.getScheduleStore(getSiteId())
            .getScheduleById(id);
        ZoneId timeZone = getTimeZoneOfSchedule(sched);
        StoredProject proj = rm.getProjectStore(getSiteId())
            .getProjectById(sched.getProjectId());
        return RestModels.schedule(sched, proj, timeZone);
    }

    @POST
    @Consumes("application/json")
    @Path("/api/schedules/{id}/skip")
    public RestScheduleSummary skipSchedule(@PathParam("id") int id, RestScheduleSkipRequest request)
        throws ResourceNotFoundException, ResourceConflictException
    {
        Preconditions.checkArgument(request.getNextTime().isPresent() || (request.getCount().isPresent() && request.getFromTime().isPresent()), "nextTime or (fromTime and count) are required");

        StoredSchedule sched = sm.getScheduleStore(getSiteId())
            .getScheduleById(id);
        ZoneId timeZone = getTimeZoneOfSchedule(sched);

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

        return RestModels.scheduleSummary(updated, timeZone);
    }

    private ZoneId getTimeZoneOfSchedule(StoredSchedule sched)
        throws ResourceNotFoundException
    {
        // TODO optimize
        return rm.getProjectStore(getSiteId())
            .getWorkflowDefinitionById(sched.getWorkflowDefinitionId())
            .getTimeZone();
    }

    @POST
    @Consumes("application/json")
    @Path("/api/schedules/{id}/backfill")
    public RestSessionAttemptCollection backfillSchedule(@PathParam("id") int id, RestScheduleBackfillRequest request)
        throws ResourceNotFoundException, ResourceConflictException, ResourceLimitExceededException
    {
        List<StoredSessionAttemptWithSession> attempts = exec.backfill(getSiteId(), id, request.getFromTime(), request.getAttemptName(), request.getCount(), request.getDryRun());

        return RestModels.attemptCollection(rm.getProjectStore(getSiteId()), attempts);
    }

    @POST
    @Path("/api/schedules/{id}/disable")
    public RestScheduleSummary disableSchedule(@PathParam("id") int id)
            throws ResourceNotFoundException, ResourceConflictException
    {
        // TODO: this is racy
        StoredSchedule sched = sm.getScheduleStore(getSiteId())
                .getScheduleById(id);
        ZoneId timeZone = getTimeZoneOfSchedule(sched);

        StoredSchedule updated = sm.getScheduleStore(getSiteId()).updateScheduleById(id, (store, storedSchedule) -> {
            ScheduleControl lockedSched = new ScheduleControl(store, storedSchedule);
            lockedSched.disableSchedule();
            return lockedSched.get();
        });

        return RestModels.scheduleSummary(updated, timeZone);
    }

    @POST
    @Path("/api/schedules/{id}/enable")
    public RestScheduleSummary enableSchedule(@PathParam("id") int id)
            throws ResourceNotFoundException, ResourceConflictException
    {
        // TODO: this is racy
        StoredSchedule sched = sm.getScheduleStore(getSiteId())
                .getScheduleById(id);
        ZoneId timeZone = getTimeZoneOfSchedule(sched);

        StoredSchedule updated = sm.getScheduleStore(getSiteId()).updateScheduleById(id, (store, storedSchedule) -> {
            ScheduleControl lockedSched = new ScheduleControl(store, storedSchedule);
            lockedSched.enableSchedule();
            return lockedSched.get();
        });

        return RestModels.scheduleSummary(updated, timeZone);
    }
}
