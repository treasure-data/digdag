package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionCollection;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptCollection;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionWithLastAttempt;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import java.util.List;
import java.util.stream.Collectors;

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
    private final TransactionManager tm;

    @Inject
    public SessionResource(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            TransactionManager tm)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
    }

    @GET
    @Path("/api/sessions")
    public RestSessionCollection getSessions(@QueryParam("last_id") Long lastId)
            throws Exception
    {
        return tm.begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            List<StoredSessionWithLastAttempt> sessions = ss.getSessions(100, Optional.fromNullable(lastId));

            return RestModels.sessionCollection(rs, sessions);
        });
    }

    @GET
    @Path("/api/sessions/{id}")
    public RestSession getSession(@PathParam("id") long id)
            throws Exception
    {
        return tm.begin(() -> {
            StoredSessionWithLastAttempt session = sm.getSessionStore(getSiteId())
                    .getSessionById(id);

            StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(session.getProjectId());

            return RestModels.session(session, proj.getName());
        });
    }

    @GET
    @Path("/api/sessions/{id}/attempts")
    public RestSessionAttemptCollection getSessionAttempts(
            @PathParam("id") long id,
            @QueryParam("last_id") Long lastId)
            throws Exception
    {
        return tm.begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            StoredSession session = ss.getSessionById(id);
            StoredProject project = rs.getProjectById(session.getProjectId());
            List<StoredSessionAttempt> attempts = ss.getAttemptsOfSession(id, 100, Optional.fromNullable(lastId));

            List<RestSessionAttempt> collection = attempts.stream()
                    .map(attempt -> RestModels.attempt(session, attempt, project.getName()))
                    .collect(Collectors.toList());

            return RestModels.attemptCollection(collection);
        });
    }
}
