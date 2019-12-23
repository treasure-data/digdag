package io.digdag.server.rs;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.function.Supplier;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import com.google.inject.Inject;
import com.google.common.base.Throwables;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.workflow.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.*;
import io.digdag.core.config.YamlConfigLoader;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.client.api.*;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

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

    @Inject
    public WorkflowResource(
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            SchedulerManager srm,
            TransactionManager tm)
    {
        this.rm = rm;
        this.sm = sm;
        this.srm = srm;
        this.tm = tm;
    }

    @Deprecated
    @GET
    @Path("/api/workflow")
    public RestWorkflowDefinition getWorkflowDefinition(
            @QueryParam("project") String projName,
            @QueryParam("revision") String revName,
            @QueryParam("name") String wfName)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            Preconditions.checkArgument(projName != null, "project= is required");
            Preconditions.checkArgument(wfName != null, "name= is required");

            ProjectStore rs = rm.getProjectStore(getSiteId());
            StoredProject proj = rs.getProjectByName(projName);
            StoredRevision rev;
            if (revName == null) {
                rev = rs.getLatestRevision(proj.getId());
            }
            else {
                rev = rs.getRevisionByName(proj.getId(), revName);
            }
            StoredWorkflowDefinition def = rs.getWorkflowDefinitionByName(rev.getId(), wfName);
            return RestModels.workflowDefinition(proj, rev, def);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/workflows")
    @ApiOperation("List workflows")
    public RestWorkflowDefinitionCollection getWorkflowDefinitions(
            @ApiParam(value="list workflows whose id is grater than this id for pagination", required=false)
            @QueryParam("last_id") Long lastId,
            @ApiParam(value="number of workflows to return", required=false)
            @QueryParam("count") Integer count)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            List<StoredWorkflowDefinitionWithProject> defs =
                    rm.getProjectStore(getSiteId())
                            .getLatestActiveWorkflowDefinitions(Optional.fromNullable(count).or(100), Optional.fromNullable(lastId));
            return RestModels.workflowDefinitionCollection(defs);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/workflows/{id}")
    @ApiOperation("Get a workflow")
    public RestWorkflowDefinition getWorkflowDefinition(
            @ApiParam(value="workflow id", required=true)
            @PathParam("id") long id)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            StoredWorkflowDefinitionWithProject def =
                    rm.getProjectStore(getSiteId())
                            .getWorkflowDefinitionById(id);
            return RestModels.workflowDefinition(def);
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/workflows/{id}/truncated_session_time")
    @ApiOperation("Get truncated local time based on the time zone of a workflow")
    public RestWorkflowSessionTime getWorkflowDefinition(
            @ApiParam(value="workflow id", required=true)
            @PathParam("id") long id,
            @ApiParam(value="session time to be truncated", required=true)
            @QueryParam("session_time") LocalTimeOrInstant localTime,
            @ApiParam(value="truncation mode - second, minute, hour, day, schedule, or next_schedule", required=false)
            @QueryParam("mode") SessionTimeTruncate mode)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            Preconditions.checkArgument(localTime != null, "session_time= is required");

            StoredWorkflowDefinitionWithProject def =
                    rm.getProjectStore(getSiteId())
                            .getWorkflowDefinitionById(id);

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
        }, ResourceNotFoundException.class);
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
