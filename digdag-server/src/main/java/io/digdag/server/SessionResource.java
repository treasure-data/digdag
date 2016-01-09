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
    // [ ] POST /api/sessions/{id}                               # do operations on a session (cancel, retry, etc.)

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
                StoredWorkflowSource wf = rs.getWorkflowByName(repo.getId(), wfName);
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
    public RestSession startTask(RestSessionRequest request)
        throws ResourceNotFoundException, ResourceConflictException, TaskMatchPattern.MultipleMatchException, TaskMatchPattern.NoMatchException
    {
        StoredWorkflowSource wf;
        SessionRelation rel;

        // TODO
        //RepositoryStore rs = rm.getRepositoryStore(siteId);
        //if (request.getWorkflowId().isPresent()) {
        //    Preconditions.checkArgument(!request.getRepositoryName().isPresent(), "repository and workflowId can't be set together");
        //    Preconditions.checkArgument(!request.getWorkflowName().isPresent(), "workflow and workflowId can't be set together");
        //    Preconditions.checkArgument(!request.getRevision().isPresent(), "revision and workflowId can't be set together");

        //    wf = rs.getWorkflowById(request.getWorkflowId().get());  // validate site id
        //    StoredWorkflowSourceWithRepository details = rm.getWorkflowDetailsById(wf.getId());
        //    rel = SessionRelation.ofWorkflow(details.getRepository().getId(), details.getRevisionId(), details.getId());
        //}
        //else {
        //    Preconditions.checkArgument(request.getRepositoryName().isPresent(), "repository is required if workflowId is not set");
        //    Preconditions.checkArgument(request.getWorkflowName().isPresent(), "workflow is required if workflowId is not set");
        //    StoredRepository repo = rs.getRepositoryByName(request.getWorkflowName().get());
        //    StoredRevision rev;
        //    if (request.getRevision().isPresent()) {
        //        rev = rs.getRevisionByName(repo.getId(), request.getRevision().get());
        //    }
        //    else {
        //        rev = rs.getLatestActiveRevision(repo.getId());
        //    }
        //    wf = rs.getWorkflowByName(rev.getId(), request.getWorkflowName().get());
        //    rel = SessionRelation.ofWorkflow(repo.getId(), rev.getId(), wf.getId());
        //}
        RepositoryStore rs = rm.getRepositoryStore(siteId);
        {
            StoredRepository repo = rs.getRepositoryByName("default");
            StoredRevision rev = rs.getLatestActiveRevision(repo.getId());
            wf = rs.getWorkflowByName(rev.getId(), request.getWorkflowName());
            rel = SessionRelation.ofWorkflow(repo.getId(), rev.getId(), wf.getId());
        }

        //String sessionName = request.getSessionName().or(UUID.randomUUID().toString());
        String sessionName = UUID.randomUUID().toString();

        Config sessionParams = cf.create();
        //if (request.getSessionParams().isPresent()) {
        //    sessionParams.setAll(request.getSessionParams().get());
        //}

        Session session = Session.sessionBuilder()
            .name(sessionName)
            .params(sessionParams)
            .options(SessionOptions.empty())
            .build();

        StoredSession stored = executor.submitWorkflow(
                siteId, wf, session, Optional.of(rel),
                new Date(), /*request.getFromTaskName().transform(name -> new TaskMatchPattern(name))*/ Optional.absent());

        return RestSession.of(stored);
    }
}
