package io.digdag.server.rs;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.api.RestSession;
import io.digdag.client.api.RestSessionCollection;
import io.digdag.client.api.RestSessionAttempt;
import io.digdag.client.api.RestSessionAttemptCollection;
import io.digdag.client.config.Config;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionWithLastAttempt;
import io.digdag.metrics.DigdagTimed;
import io.digdag.spi.ac.AccessControlException;
import io.digdag.spi.ac.AccessController;
import io.digdag.spi.ac.SiteTarget;
import io.digdag.spi.ac.WorkflowTarget;
import io.digdag.spi.metrics.DigdagMetrics;
import io.swagger.annotations.Api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import java.util.List;
import java.util.stream.Collectors;

@Api("Session")
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
    private final AccessController ac;
    private static int MAX_SESSIONS_PAGE_SIZE;
    private static final int DEFAULT_SESSIONS_PAGE_SIZE = 100;
    private static int MAX_ATTEMPTS_PAGE_SIZE;
    private static final int DEFAULT_ATTEMPTS_PAGE_SIZE = 100;
    private final DigdagMetrics metrics;

    @Inject
    public SessionResource(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            TransactionManager tm,
            AccessController ac,
            Config systemConfig,
            DigdagMetrics metrics)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        this.ac = ac;
        this.metrics = metrics;
        MAX_SESSIONS_PAGE_SIZE = systemConfig.get("api.max_sessions_page_size", Integer.class, DEFAULT_SESSIONS_PAGE_SIZE);
        MAX_ATTEMPTS_PAGE_SIZE = systemConfig.get("api.max_attempts_page_size", Integer.class, DEFAULT_ATTEMPTS_PAGE_SIZE);
    }


    @DigdagTimed(value="GetSessions", category="api")
    @GET
    @Path("/api/sessions")
    public RestSessionCollection getSessions(
            @QueryParam("last_id") Long lastId,
            @QueryParam("page_size") Integer pageSize)
            throws AccessControlException
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_SESSIONS_PAGE_SIZE, DEFAULT_SESSIONS_PAGE_SIZE);

        final SiteTarget siteTarget = SiteTarget.of(getSiteId());
        ac.checkListSessionsOfSite( // AccessControl
                siteTarget,
                getAuthenticatedUser());

        return tm.begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            // of site
            List<StoredSessionWithLastAttempt> sessions = ss.getSessions(validPageSize, Optional.fromNullable(lastId),
                    ac.getListSessionsFilterOfSite(
                            siteTarget,
                            getAuthenticatedUser()));

            return RestModels.sessionCollection(rs, sessions);
        });
    }

    @DigdagTimed(value="GetSessionById", category="api")
    @GET
    @Path("/api/sessions/{id}")
    public RestSession getSession(@PathParam("id") long id)
            throws ResourceNotFoundException, AccessControlException
    {
        return tm.<RestSession, ResourceNotFoundException, AccessControlException>begin(() -> {
            final StoredSessionWithLastAttempt session = sm.getSessionStore(getSiteId())
                    .getSessionById(id); // check NotFound first
            final StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(session.getProjectId()); // check NotFound first

            ac.checkGetSession( // AccessControl
                    WorkflowTarget.of(getSiteId(), session.getWorkflowName(), proj.getName()),
                    getAuthenticatedUser());

            return RestModels.session(session, proj.getName());
        }, ResourceNotFoundException.class, AccessControlException.class);
    }

    @DigdagTimed(value="GetSessionAttempts", category="api")
    @GET
    @Path("/api/sessions/{id}/attempts")
    public RestSessionAttemptCollection getSessionAttempts(
            @PathParam("id") long id,
            @QueryParam("last_id") Long lastId,
            @QueryParam("page_size") Integer pageSize)
            throws ResourceNotFoundException, AccessControlException
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_ATTEMPTS_PAGE_SIZE, DEFAULT_ATTEMPTS_PAGE_SIZE);

        return tm.<RestSessionAttemptCollection, ResourceNotFoundException, AccessControlException>begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            final StoredSession session = ss.getSessionById(id); // check NotFound first
            final StoredProject project = rs.getProjectById(session.getProjectId()); // check NotFound first

            ac.checkGetAttemptsFromSession( // AccessControl
                    WorkflowTarget.of(getSiteId(), session.getWorkflowName(), project.getName()),
                    getAuthenticatedUser());

            List<StoredSessionAttempt> attempts = ss.getAttemptsOfSession(id, validPageSize, Optional.fromNullable(lastId));

            List<RestSessionAttempt> collection = attempts.stream()
                    .map(attempt -> RestModels.attempt(session, attempt, project.getName()))
                    .collect(Collectors.toList());

            return RestModels.attemptCollection(collection);
        }, ResourceNotFoundException.class, AccessControlException.class);
    }
}
