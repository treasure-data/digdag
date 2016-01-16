package io.digdag.server;

import java.util.List;
import java.util.Date;
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
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigFactory;
import io.digdag.client.api.*;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

@Path("/")
@Produces("application/json")
public class SessionResource
{
    // [*] GET  /api/sessions                                    # list sessions from recent to old
    // [*] GET  /api/sessions?repository=<name>                  # list sessions that belong to a particular repository
    // [*] GET  /api/sessions?repository=<name>&workflow=<name>  # list sessions that belong to a particular workflow
    // [*] GET  /api/sessions/{id}                               # show a session
    // [*] GET  /api/sessions/{id}/tasks                         # list tasks of a session
    // [*] PUT  /api/sessions                                    # starts a new session (cancel, retry, etc.)
    // [*] POST /api/sessions/{id}/kill                          # kill a session

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

    @GET
    @Path("/api/sessions")
    public List<RestSession> getSessions(@QueryParam("repository") String repoName, @QueryParam("workflow") String wfName)
        throws ResourceNotFoundException
    {
        // TODO paging
        List<StoredSession> sessions;
        RepositoryStore rs = rm.getRepositoryStore(siteId);
        SessionStore ss = sm.getSessionStore(siteId);
        if (repoName == null) {
            sessions = ss.getSessions(100, Optional.absent());
        }
        else {
            StoredRepository repo = rs.getRepositoryByName(repoName);
            if (wfName == null) {
                sessions = ss.getSessionsOfRepository(repo.getId(), 100, Optional.absent());
            }
            else {
                StoredWorkflowSource wf = rs.getWorkflowSourceByName(repo.getId(), wfName);
                sessions = ss.getSessionsOfWorkflow(wf.getId(), 100, Optional.absent());
            }
        }

        return sessions.stream()
            .map(s -> RestSession.of(s))
            .collect(Collectors.toList());
    }

    @GET
    @Path("/api/sessions/{id}")
    public RestSession getSession(@PathParam("id") long id)
        throws ResourceNotFoundException
    {
        StoredSession s = sm.getSessionStore(siteId)
            .getSessionById(id);
        return RestSession.of(s);
    }

    @GET
    @Path("/api/sessions/{id}/tasks")
    public List<RestTask> getTasks(@PathParam("id") long id)
    {
        // TODO paging
        return sm.getSessionStore(siteId)
            .getTasks(id, 100, Optional.absent())
            .stream()
            .map(task -> RestTask.of(task))
            .collect(Collectors.toList());
    }

    @PUT
    @Consumes("application/json")
    @Path("/api/sessions")
    public RestSession startSession(RestSessionRequest request)
        throws ResourceNotFoundException, ResourceConflictException, TaskMatchPattern.MultipleMatchException, TaskMatchPattern.NoMatchException
    {
        StoredRevision rev;
        StoredWorkflowSource wf;
        SessionRelation rel;

        RepositoryStore rs = rm.getRepositoryStore(siteId);

        // TODO support startSession by id
        //if (request.getWorkflowId().isPresent()) {
        //    Preconditions.checkArgument(!request.getRepositoryName().isPresent(), "repository and workflowId can't be set together");
        //    Preconditions.checkArgument(!request.getWorkflowName().isPresent(), "workflow and workflowId can't be set together");
        //    Preconditions.checkArgument(!request.getRevision().isPresent(), "revision and workflowId can't be set together");

        //    wf = rs.getWorkflowById(request.getWorkflowId().get());  // validate site id
        //    StoredWorkflowSourceWithRepository details = rm.getWorkflowDetailsById(wf.getId());
        //    rel = SessionRelation.ofWorkflow(details.getRepository().getId(), details.getRevisionId(), details.getId());
        //}
        {
            StoredRepository repo = rs.getRepositoryByName(request.getRepositoryName());
            // TODO support startSession using an old revision
            //if (request.getRevision().isPresent()) {
            //    rev = rs.getRevisionByName(repo.getId(), request.getRevision().get());
            //}
            //else {
                rev = rs.getLatestActiveRevision(repo.getId());
            //}
            wf = rs.getWorkflowSourceByName(rev.getId(), request.getWorkflowName());
            rel = SessionRelation.ofWorkflow(repo.getId(), rev.getId(), wf.getId());
        }

        Session session = Session.sessionBuilder(
                request.getName(), rev.getDefaultParams(), wf, request.getParams())
            .options(SessionOptions.empty())
            .build();

        // TODO catch TaskMatchPattern.MultipleMatchException and TaskMatchPattern.NoMatchException and throw
        //      an exception that is mapped to 422 Unprocessable Entity
        StoredSession stored = executor.submitWorkflow(
                siteId, wf, session, Optional.of(rel),
                new Date(), /*request.getFromTaskName().transform(name -> new TaskMatchPattern(name))*/ Optional.absent());

        return RestSession.of(stored);
    }

    @POST
    @Consumes("application/json")
    @Path("/api/sessions/{id}/kill")
    public void killSession(@PathParam("id") long id)
        throws ResourceNotFoundException, ResourceConflictException
    {
        boolean updated = executor.killSessionById(siteId, id);
        if (!updated) {
            throw new ResourceConflictException("Session already killed or finished");
        }
    }
}
