package io.digdag.server.rs;

import java.util.List;
import java.util.stream.Collectors;
import java.time.Instant;
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
import io.digdag.server.rs.TempFileManager.TempDir;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

@Path("/")
@Produces("application/json")
public class ScheduleResource
    extends AuthenticatedResource
{
    // [*] GET  /api/schedules                                   # list schedules of the latest revision of all repositories
    // [*] GET  /api/schedules/<id>                              # show a particular schedule (which belongs to a workflow)
    // [*] POST /api/schedules/<id>/skip                         # skips schedules forward to a future time
    // [*] POST /api/schedules/<id>/backfill                     # run or re-run past schedules

    private final RepositoryStoreManager rm;
    private final ScheduleStoreManager sm;
    private final ScheduleExecutor exec;

    @Inject
    public ScheduleResource(
            RepositoryStoreManager rm,
            ScheduleStoreManager sm,
            ScheduleExecutor exec)
    {
        this.rm = rm;
        this.sm = sm;
        this.exec = exec;
    }

    @GET
    @Path("/api/schedules")
    public List<RestSchedule> getSchedules()
    {
        // TODO paging
        RepositoryMap repos = RepositoryMap.get(rm.getRepositoryStore(getSiteId()));
        return sm.getScheduleStore(getSiteId())
            .getSchedules(100, Optional.absent())
            .stream()
            .map(sched -> {
                try {
                    return RestModels.schedule(sched, repos.get(sched.getRepositoryId()));
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
    public RestSchedule getSchedules(@PathParam("id") long id)
        throws ResourceNotFoundException
    {
        StoredSchedule sched = sm.getScheduleStore(getSiteId())
            .getScheduleById(id);
        StoredRepository repo = rm.getRepositoryStore(getSiteId())
            .getRepositoryById(sched.getRepositoryId());
        return RestModels.schedule(sched, repo);
    }

    @POST
    @Consumes("application/json")
    @Path("/api/schedules/{id}/skip")
    public RestScheduleSummary skipSchedule(@PathParam("id") long id, RestScheduleSkipRequest request)
        throws ResourceNotFoundException, ResourceConflictException
    {
        StoredSchedule updated;
        if (request.getNextTime().isPresent()) {
            updated = exec.skipScheduleToTime(getSiteId(), id,
                    Instant.ofEpochSecond(request.getNextTime().get()),
                    request.getNextRunTime().transform(t -> Instant.ofEpochSecond(t)),
                    request.getDryRun());
        }
        else {
            updated = exec.skipScheduleByCount(getSiteId(), id,
                    Instant.ofEpochSecond(request.getFromTime().get()),
                    request.getCount().get(),
                    request.getNextRunTime().transform(t -> Instant.ofEpochSecond(t)),
                    request.getDryRun());
        }
        return RestModels.scheduleSummary(updated);
    }

    @POST
    @Consumes("application/json")
    @Path("/api/schedules/{id}/backfill")
    public List<RestSession> backfillSchedule(@PathParam("id") long id, RestScheduleBackfillRequest request)
        throws ResourceNotFoundException, ResourceConflictException
    {
        List<StoredSessionAttemptWithSession> attempts = exec.backfill(getSiteId(), id, Instant.ofEpochSecond(request.getFromTime()), request.getAttemptName(), request.getDryRun());

        RepositoryMap repos = RepositoryMap.get(rm.getRepositoryStore(getSiteId()));

        return attempts.stream()
            .map(attempt -> {
                try {
                    return RestModels.session(attempt, repos.get(attempt.getSession().getRepositoryId()).getName());
                }
                catch (ResourceNotFoundException ex) {
                    // must not happen
                    return null;
                }
            })
            .filter(a -> a != null)
            .collect(Collectors.toList());
    }
}
