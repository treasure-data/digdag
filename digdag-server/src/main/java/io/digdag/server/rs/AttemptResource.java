package io.digdag.server.rs;

import java.util.List;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.GET;
import javax.ws.rs.core.Response;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.inject.Inject;
import com.google.common.collect.*;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSession;
import io.digdag.core.workflow.*;
import io.digdag.core.session.*;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.api.*;
import io.digdag.spi.Scheduler;
import io.digdag.spi.ScheduleTime;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

@Path("/")
@Produces("application/json")
public class AttemptResource
    extends AuthenticatedResource
{
    // [*] GET  /api/attempts                                    # list attempts from recent to old
    // [*] GET  /api/attempts?include_retried=1                  # list attempts from recent to old
    // [*] GET  /api/attempts?repository=<name>                  # list attempts that belong to a particular repository
    // [*] GET  /api/attempts?repository=<name>&workflow=<name>  # list attempts that belong to a particular workflow
    // [*] GET  /api/attempts/{id}                               # show a session
    // [*] GET  /api/attempts/{id}/tasks                         # list tasks of a session
    // [*] GET  /api/attempts/{id}/retries                       # list retried attempts of this session
    // [*] GET  /api/prepare                                     # prepares parameters for a new session attempt
    // [*] PUT  /api/attempts                                    # starts a new session
    // [*] POST /api/attempts/{id}/kill                          # kill a session

    private final RepositoryStoreManager rm;
    private final SessionStoreManager sm;
    private final SchedulerManager srm;
    private final AttemptBuilder attemptBuilder;
    private final WorkflowExecutor executor;
    private final ConfigFactory cf;

    @Inject
    public AttemptResource(
            RepositoryStoreManager rm,
            SessionStoreManager sm,
            SchedulerManager srm,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor executor,
            ConfigFactory cf)
    {
        this.rm = rm;
        this.sm = sm;
        this.srm = srm;
        this.attemptBuilder = attemptBuilder;
        this.executor = executor;
        this.cf = cf;
    }

    @GET
    @Path("/api/attempts")
    public List<RestSessionAttempt> getAttempts(
            @QueryParam("repository") String repoName,
            @QueryParam("workflow") String wfName,
            @QueryParam("include_retried") boolean includeRetried,
            @QueryParam("last_id") Long lastId)
        throws ResourceNotFoundException
    {
        List<StoredSessionAttemptWithSession> attempts;

        RepositoryStore rs = rm.getRepositoryStore(getSiteId());
        SessionStore ss = sm.getSessionStore(getSiteId());
        if (repoName != null) {
            StoredRepository repo = rs.getRepositoryByName(repoName);
            if (wfName != null) {
                // of workflow
                StoredWorkflowDefinition wf = rs.getLatestWorkflowDefinitionByName(repo.getId(), wfName);
                attempts = ss.getSessionsOfWorkflow(includeRetried, wf.getId(), 100, Optional.fromNullable(lastId));
            }
            else {
                // of repository
                attempts = ss.getSessionsOfRepository(includeRetried, repo.getId(), 100, Optional.fromNullable(lastId));
            }
        }
        else {
            // of site
            attempts = ss.getSessions(includeRetried, 100, Optional.fromNullable(lastId));
        }

        RepositoryMap repos = RepositoryMap.get(rm.getRepositoryStore(getSiteId()));

        return attempts.stream()
            .map(attempt -> {
                try {
                    return RestModels.attempt(attempt, repos.get(attempt.getSession().getRepositoryId()).getName());
                }
                catch (ResourceNotFoundException ex) {
                    return null;
                }
            })
            .filter(a -> a != null)
            .collect(Collectors.toList());
    }

    @GET
    @Path("/api/attempts/{id}")
    public RestSessionAttempt getAttempt(@PathParam("id") long id)
        throws ResourceNotFoundException
    {
        StoredSessionAttemptWithSession attempt = sm.getSessionStore(getSiteId())
            .getSessionAttemptById(id);

        RepositoryMap repos = RepositoryMap.get(rm.getRepositoryStore(getSiteId()));

        return RestModels.attempt(attempt, repos.get(attempt.getSession().getRepositoryId()).getName());
    }

    @GET
    @Path("/api/attempts/{id}/retries")
    public List<RestSessionAttempt> getAttemptRetries(@PathParam("id") long id)
        throws ResourceNotFoundException
    {
        List<StoredSessionAttemptWithSession> attempts = sm.getSessionStore(getSiteId())
            .getOtherAttempts(id);

        RepositoryMap repos = RepositoryMap.get(rm.getRepositoryStore(getSiteId()));

        return attempts.stream()
            .map(attempt -> {
                try {
                    return RestModels.attempt(attempt, repos.get(attempt.getSession().getRepositoryId()).getName());
                }
                catch (ResourceNotFoundException ex) {
                    return null;
                }
            })
            .filter(a -> a != null)
            .collect(Collectors.toList());
    }

    @GET
    @Path("/api/attempts/{id}/tasks")
    public List<RestTask> getTasks(@PathParam("id") long id)
    {
        return sm.getSessionStore(getSiteId())
            .getTasksOfAttempt(id)
            .stream()
            .map(task -> RestModels.task(task))
            .collect(Collectors.toList());
    }

    @GET
    @Consumes("application/json")
    @Path("/api/prepare")
    public RestSessionAttemptPrepareResult prepareAttempt(
            @QueryParam("repository") String repoName,
            @QueryParam("revision") String revName,
            @QueryParam("workflow") String wfName,
            @QueryParam("session_time") LocalTimeOrInstant localTime,
            @QueryParam("session_time_truncate") SessionTimeTruncate sessionTimeTruncate)
        throws ResourceNotFoundException
    {
        Preconditions.checkArgument(repoName != null, "repository= is required");
        // revName is nullable
        Preconditions.checkArgument(wfName != null, "workflow= is required");
        Preconditions.checkArgument(localTime != null, "session_time_ is required");
        // sessionTimeTruncate is nullable

        RepositoryStore rs = rm.getRepositoryStore(getSiteId());

        String resolvedRevision;
        long resolvedWorkflowId;

        ZoneId timeZone;
        Supplier<Optional<Scheduler>> schedulerSupplier;

        StoredRepository repo = rs.getRepositoryByName(repoName);
        if (revName != null) {
            StoredRevision rev = rs.getRevisionByName(repo.getId(), revName);
            StoredWorkflowDefinition def = rs.getWorkflowDefinitionByName(rev.getId(), wfName);
            resolvedRevision = rev.getName();
            resolvedWorkflowId = def.getId();
            timeZone = ScheduleExecutor.getTimeZoneOfStoredWorkflow(rev, def);
            schedulerSupplier = () -> srm.tryGetScheduler(rev, def);
        }
        else {
            StoredWorkflowDefinitionWithRepository details = rs.getLatestWorkflowDefinitionByName(repo.getId(), wfName);
            resolvedRevision = details.getRevisionName();
            resolvedWorkflowId = details.getId();
            timeZone = ScheduleExecutor.getTimeZoneOfStoredWorkflow(details);
            schedulerSupplier = () -> srm.tryGetScheduler(details);
        }

        Instant resolvedSessionTime;

        if (sessionTimeTruncate != null) {
            resolvedSessionTime = truncateSessionTime(localTime.toInstant(timeZone),
                    timeZone, schedulerSupplier, sessionTimeTruncate);
        }
        else {
            resolvedSessionTime = localTime.toInstant(timeZone);
        }

        return RestSessionAttemptPrepareResult.builder()
            .workflowId(resolvedWorkflowId)
            .revision(resolvedRevision)
            .sessionTime(resolvedSessionTime)
            .build();
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
                    throw new IllegalArgumentException("session_time_truncate=" + mode + " is set but _schedule is not set to this workflow");
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

    @PUT
    @Consumes("application/json")
    @Path("/api/attempts")
    public Response startAttempt(RestSessionAttemptRequest request)
        throws ResourceNotFoundException, ResourceConflictException
    {
        RepositoryStore rs = rm.getRepositoryStore(getSiteId());

        StoredWorkflowDefinitionWithRepository def = rs.getWorkflowDefinitionById(request.getWorkflowId());

        // use the HTTP request time as the runTime
        AttemptRequest ar = attemptBuilder.buildFromStoredWorkflow(
                def,
                request.getParams(),
                ScheduleTime.runNow(request.getSessionTime()),
                request.getRetryAttemptName());

        try {
            StoredSessionAttemptWithSession attempt = executor.submitWorkflow(getSiteId(), ar, def);
            RestSessionAttempt res = RestModels.attempt(attempt, def.getRepository().getName());
            return Response.ok(res).build();
        }
        catch (SessionAttemptConflictException ex) {
            StoredSessionAttemptWithSession conflicted = ex.getConflictedSession();
            RestSessionAttempt res = RestModels.attempt(conflicted, def.getRepository().getName());
            return Response.status(Response.Status.CONFLICT).entity(res).build();
        }
    }

    @POST
    @Consumes("application/json")
    @Path("/api/attempts/{id}/kill")
    public void killAttempt(@PathParam("id") long id)
        throws ResourceNotFoundException, ResourceConflictException
    {
        boolean updated = executor.killAttemptById(getSiteId(), id);
        if (!updated) {
            throw new ResourceConflictException("Session attempt already killed or finished");
        }
    }
}
