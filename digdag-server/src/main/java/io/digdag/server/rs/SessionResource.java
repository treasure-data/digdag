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
import io.swagger.annotations.Api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import java.util.ArrayList;
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
    private static int MAX_SESSIONS_PAGE_SIZE;
    private static final int DEFAULT_SESSIONS_PAGE_SIZE = 100;
    private static int MAX_ATTEMPTS_PAGE_SIZE;
    private static final int DEFAULT_ATTEMPTS_PAGE_SIZE = 100;
    private static final int DEFAULT_PAGE_NUMBER = 1;

    @Inject
    public SessionResource(
            ProjectStoreManager rm,
            SessionStoreManager sm,
            TransactionManager tm,
            Config systemConfig)
    {
        this.rm = rm;
        this.sm = sm;
        this.tm = tm;
        MAX_SESSIONS_PAGE_SIZE = systemConfig.get("api.max_sessions_page_size", Integer.class, DEFAULT_SESSIONS_PAGE_SIZE);
        MAX_ATTEMPTS_PAGE_SIZE = systemConfig.get("api.max_attempts_page_size", Integer.class, DEFAULT_ATTEMPTS_PAGE_SIZE);
    }

    @GET
    @Path("/api/sessions")
    public RestSessionCollection getSessions(
            @QueryParam("last_id") Long lastId,
            @QueryParam("page_size") Integer pageSize,
            @QueryParam("page_number") Integer pageNumber)
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_SESSIONS_PAGE_SIZE, DEFAULT_SESSIONS_PAGE_SIZE);
        int validPageNumber = QueryParamValidator.validatePageNumber(Optional.fromNullable(pageNumber), DEFAULT_PAGE_NUMBER);

        return tm.begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            Integer sessionRecordsNumber = ss.getSessionsCount(Optional.fromNullable(lastId));

            List<StoredSessionWithLastAttempt> sessions = ss.getSessions(validPageSize, Optional.fromNullable(lastId), validPageNumber);
            return RestModels.sessionCollection(rs, sessions, sessionRecordsNumber);
        });
    }

    @GET
    @Path("/api/sessions/{id}")
    public RestSession getSession(@PathParam("id") long id)
            throws ResourceNotFoundException
    {
        return tm.begin(() -> {
            StoredSessionWithLastAttempt session = sm.getSessionStore(getSiteId())
                    .getSessionById(id);

            StoredProject proj = rm.getProjectStore(getSiteId())
                    .getProjectById(session.getProjectId());

            return RestModels.session(session, proj.getName());
        }, ResourceNotFoundException.class);
    }

    @GET
    @Path("/api/sessions/{id}/attempts")
    public RestSessionAttemptCollection getSessionAttempts(
            @PathParam("id") long id,
            @QueryParam("last_id") Long lastId,
            @QueryParam("page_size") Integer pageSize)
            throws ResourceNotFoundException
    {
        int validPageSize = QueryParamValidator.validatePageSize(Optional.fromNullable(pageSize), MAX_ATTEMPTS_PAGE_SIZE, DEFAULT_ATTEMPTS_PAGE_SIZE);

        return tm.begin(() -> {
            ProjectStore rs = rm.getProjectStore(getSiteId());
            SessionStore ss = sm.getSessionStore(getSiteId());

            StoredSession session = ss.getSessionById(id);
            StoredProject project = rs.getProjectById(session.getProjectId());
            List<StoredSessionAttempt> attempts = ss.getAttemptsOfSession(id, validPageSize, Optional.fromNullable(lastId));

            List<RestSessionAttempt> collection = attempts.stream()
                    .map(attempt -> RestModels.attempt(session, attempt, project.getName()))
                    .collect(Collectors.toList());

            return RestModels.attemptCollection(collection);
        }, ResourceNotFoundException.class);
    }
}
