package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.StoredSessionWithLastAttempt;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import java.util.List;

import static io.digdag.server.rs.RestModels.attemptModels;

@Path("/")
@Produces("application/json")
public class SessionResource
        extends AuthenticatedResource
{
    // GET  /api/sessions                                    # List sessions from recent to old
    // GET  /api/sessions/{id}                               # Get a session by id
    // GET  /api/sessions/{id}/attempts                      # List attempts of a session

    private final ProjectStoreManager rm;
    private final SessionStoreManager sm;

    @Inject
    public SessionResource(
            ProjectStoreManager rm,
            SessionStoreManager sm)
    {
        this.rm = rm;
        this.sm = sm;
    }

    @GET
    @Path("/api/sessions")
    public List<RestSession> getSessions(@QueryParam("last_id") Long lastId)
            throws ResourceNotFoundException
    {
        ProjectStore rs = rm.getProjectStore(getSiteId());
        SessionStore ss = sm.getSessionStore(getSiteId());

        List<StoredSessionWithLastAttempt> sessions = ss.getSessions(100, Optional.fromNullable(lastId));

        return RestModels.sessionModels(rs, sessions);
    }

    @GET
    @Path("/api/sessions/{id}")
    public RestSession getSession(@PathParam("id") long id)
            throws ResourceNotFoundException
    {
        StoredSessionWithLastAttempt session = sm.getSessionStore(getSiteId())
                .getSessionById(id);

        StoredProject proj = rm.getProjectStore(getSiteId())
                .getProjectById(session.getProjectId());

        return RestModels.session(session, proj.getName());
    }

    @GET
    @Path("/api/sessions/{id}/attempts")
    public List<RestSessionAttempt> getSessionAttempts(
            @PathParam("id") long id,
            @QueryParam("last_id") Long lastId)
            throws ResourceNotFoundException
    {

        ProjectStore rs = rm.getProjectStore(getSiteId());
        SessionStore ss = sm.getSessionStore(getSiteId());

        List<StoredSessionAttemptWithSession> attempts = ss.getAttemptsOfSession(true, id, 100, Optional.fromNullable(lastId));

        return attemptModels(rm, getSiteId(), attempts);
    }
}
