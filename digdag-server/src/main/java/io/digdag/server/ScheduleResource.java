package io.digdag.server;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
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
import io.digdag.client.api.*;
import io.digdag.server.TempFileManager.TempDir;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

@Path("/")
@Produces("application/json")
public class ScheduleResource
{
    // [*] GET  /api/schedules                                   # list schedules of the latest revision of all repositories
    // [*] GET  /api/schedules/<id>                              # show a particular schedule (which belongs to a workflow)
    // [*] POST /api/schedules/<id>/skip                         # skips schedules forward to a future time
    // [ ] POST /api/schedules/<id>/backfill                     # run or re-run past schedules

    private final RepositoryStoreManager rm;
    private final ScheduleStoreManager sm;
    private final ScheduleExecutor exec;

    private int siteId = 0;  // TODO get site id from context

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
        // TODO n-m db access
        return sm.getScheduleStore(siteId)
            .getSchedules(100, Optional.absent())
            .stream()
            .map(sched -> {
                try {
                    StoredWorkflowSource wf = rm.getRepositoryStore(siteId).getWorkflowSourceById(sched.getWorkflowSourceId());
                    return RestModels.schedule(sched, wf);
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
        StoredSchedule sched = sm.getScheduleStore(siteId).getScheduleById(id);
        StoredWorkflowSource wf = rm.getRepositoryStore(siteId).getWorkflowSourceById(sched.getWorkflowSourceId());
        return RestModels.schedule(sched, wf);
    }

    @POST
    @Consumes("application/json")
    @Path("/api/schedules/{id}/skip")
    public RestScheduleSummary skipSchedule(@PathParam("id") long id, RestScheduleSkipRequest request)
        throws ResourceNotFoundException, ResourceConflictException
    {
        StoredSchedule updated;
        if (request.getNextTime().isPresent()) {
            updated = exec.skipScheduleToTime(siteId, id,
                    new Date(request.getNextTime().get() * 1000),
                    request.getNextRunTime().transform(t -> new Date(t * 1000)),
                    request.getDryRun());
        }
        else {
            updated = exec.skipScheduleByCount(siteId, id,
                    new Date(request.getFromTime().get() * 1000),
                    request.getCount().get(),
                    request.getNextRunTime().transform(t -> new Date(t * 1000)),
                    request.getDryRun());
        }
        return RestModels.scheduleSummary(updated);
    }
}
