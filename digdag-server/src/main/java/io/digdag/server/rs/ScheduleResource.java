package io.digdag.server.rs;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.time.Instant;
import java.time.ZoneId;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayInputStream;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import com.google.inject.Inject;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.client.api.*;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

@Path("/")
@Produces("application/json")
public class ScheduleResource
    extends AuthenticatedResource
{
    // GET  /api/schedules                                   # list schedules of the latest revision of all projects
    // GET  /api/schedules/{id}                              # show a particular schedule (which belongs to a workflow)
    // POST /api/schedules/{id}/skip                         # skips schedules forward to a future time
    // POST /api/schedules/{id}/backfill                     # run or re-run past schedules

    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final ScheduleExecutor exec;

    @Inject
    public ScheduleResource(
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            ScheduleExecutor exec)
    {
        this.rm = rm;
        this.sm = sm;
        this.exec = exec;
    }

    @GET
    @Path("/api/schedules")
    public List<RestSchedule> getSchedules(@QueryParam("last_id") Integer lastId)
    {
        List<StoredSchedule> scheds = sm.getScheduleStore(getSiteId())
            .getSchedules(100, Optional.fromNullable(lastId));

        ProjectMap projs = rm.getProjectStore(getSiteId())
            .getProjectsByIdList(
                    scheds.stream()
                    .map(StoredSchedule::getProjectId)
                    .collect(Collectors.toList()));
        TimeZoneMap defTimeZones = rm.getProjectStore(getSiteId())
            .getWorkflowTimeZonesByIdList(
                    scheds.stream()
                    .map(StoredSchedule::getWorkflowDefinitionId)
                    .collect(Collectors.toList()));

        return scheds.stream()
            .map(sched -> {
                try {
                    return RestModels.schedule(sched,
                            projs.get(sched.getProjectId()),
                            defTimeZones.get(sched.getWorkflowDefinitionId()));
                }
                catch (ResourceNotFoundException ex) {
                    return null;
                }
            })
            .filter(sched -> sched != null)
            .collect(Collectors.toList());
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
    public List<RestSessionAttempt> backfillSchedule(@PathParam("id") int id, RestScheduleBackfillRequest request)
        throws ResourceNotFoundException, ResourceConflictException
    {
        List<StoredSessionAttemptWithSession> attempts = exec.backfill(getSiteId(), id, request.getFromTime(), request.getAttemptName(), request.getDryRun());

        return AttemptResource.attemptModels(rm, getSiteId(), attempts);
    }
}
