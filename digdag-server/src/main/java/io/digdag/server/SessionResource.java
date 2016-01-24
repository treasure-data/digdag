package io.digdag.server;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
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
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.api.*;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

@Path("/")
@Produces("application/json")
public class SessionResource
{
    // all sessions joined with the latest attempt. returned id is attempt id
    // [*] GET  /api/sessions                                    # list sessions from recent to old
    // [ ] GET  /api/sessions?include_retried=1                  # list sessions from recent to old
    //
    // all sessions joined with the latest attempt, repository_id=X. returned id is attempt id
    // [*] GET  /api/sessions?repository=<name>                  # list sessions that belong to a particular repository
    //
    // all sessions joined with the latest attempt, repository_id=X, workflow_name=X
    // [*] GET  /api/sessions?repository=<name>&workflow=<name>  # list sessions that belong to a particular workflow
    //
    // return an attempt
    // [*] GET  /api/sessions/{id}                               # show a session
    //
    // return tasks of an attempt
    // [*] GET  /api/sessions/{id}/tasks                         # list tasks of a session
    //
    // return all retries of the attempt
    // [*] GET  /api/sessions/{id}/retries
    //
    // [*] PUT  /api/sessions                                    # starts a new session (cancel, retry, etc.)
    //
    // [*] POST /api/sessions/{id}/kill                          # kill a session
    //

    private final RepositoryStoreManager rm;
    private final SessionStoreManager sm;
    private final WorkflowExecutor executor;
    private final ConfigFactory cf;

    private int siteId = 0;  // TODO get site id from context

    @Inject
    public SessionResource(
            RepositoryStoreManager rm,
            SessionStoreManager sm,
            WorkflowExecutor executor,
            ConfigFactory cf)
    {
        this.rm = rm;
        this.sm = sm;
        this.executor = executor;
        this.cf = cf;
    }

    //@GET
    //@Path("/api/sessions")
    //public List<RestSession> getSessions(@QueryParam("repository") String repoName, @QueryParam("workflow") String wfName)
    //    throws ResourceNotFoundException
    //{
    //    // TODO paging
    //    List<StoredSession> sessions;
    //    RepositoryStore rs = rm.getRepositoryStore(siteId);
    //    SessionStore ss = sm.getSessionStore(siteId);
    //    if (repoName == null) {
    //        sessions = ss.getSessions(100, Optional.absent());
    //    }
    //    else {
    //        StoredRepository repo = rs.getRepositoryByName(repoName);
    //        if (wfName == null) {
    //            sessions = ss.getSessionsOfRepository(repo.getId(), 100, Optional.absent());
    //        }
    //        else {
    //            StoredWorkflowDefinition wf = rs.getWorkflowDefinitionByName(repo.getId(), wfName);
    //            sessions = ss.getSessionsOfWorkflow(wf.getId(), 100, Optional.absent());
    //        }
    //    }

    //    return sessions.stream()
    //        .map(s -> RestModels.session(s))
    //        .collect(Collectors.toList());
    //}

    //@GET
    //@Path("/api/sessions/{id}")
    //public RestSession getSession(@PathParam("id") long id)
    //    throws ResourceNotFoundException
    //{
    //    StoredSession s = sm.getSessionStore(siteId)
    //        .getSessionById(id);
    //    return RestModels.session(s);
    //}

    //@GET
    //@Path("/api/sessions/{id}/tasks")
    //public List<RestTask> getTasks(@PathParam("id") long id)
    //{
    //    // TODO paging
    //    return sm.getSessionStore(siteId)
    //        .getTasks(id, 100, Optional.absent())
    //        .stream()
    //        .map(task -> RestModels.task(task))
    //        .collect(Collectors.toList());
    //}

    @PUT
    @Consumes("application/json")
    @Path("/api/sessions")
    public RestSession startSession(RestSessionRequest request)
        throws ResourceNotFoundException, ResourceConflictException, TaskMatchPattern.MultipleTaskMatchException, TaskMatchPattern.NoMatchException
    {
        RepositoryStore rs = rm.getRepositoryStore(siteId);

        StoredRepository repo = rs.getRepositoryByName(request.getRepositoryName());
        StoredWorkflowDefinitionWithRepository def = rs.getLatestWorkflowDefinitionByName(repo.getId(), request.getWorkflowName());

        StoredRevision rev = rs.getRevisionById(def.getRevisionId());

        AttemptRequest ar = AttemptRequest.builder()
            .repositoryId(repo.getId())
            .workflowName(def.getName())
            .instant(request.getInstant())
            .retryAttemptName(request.getRetryAttemptName())
            .defaultTimeZone(ZoneId.of("UTC"))
            .defaultParams(rev.getDefaultParams())
            .overwriteParams(request.getParams())
            .build();

        // TODO how to make session monitors?
        StoredSessionAttempt stored = executor.submitWorkflow(siteId, ar, def, ImmutableList.of());

        return RestModels.session(stored, ar, repo.getName());
    }

    //@POST
    //@Consumes("application/json")
    //@Path("/api/sessions/{id}/kill")
    //public void killSession(@PathParam("id") long id)
    //    throws ResourceNotFoundException, ResourceConflictException
    //{
    //    boolean updated = executor.killSessionById(siteId, id);
    //    if (!updated) {
    //        throw new ResourceConflictException("Session already killed or finished");
    //    }
    //}
}
