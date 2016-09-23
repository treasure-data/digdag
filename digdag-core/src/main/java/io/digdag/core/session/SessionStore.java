package io.digdag.core.session;

import java.util.List;
import java.time.Instant;
import com.google.common.base.*;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public interface SessionStore
{
    List<StoredSessionWithLastAttempt> getSessions(int pageSize, Optional<Long> lastId);

    StoredSessionWithLastAttempt getSessionById(long sessionId)
        throws ResourceNotFoundException;

    List<StoredSessionWithLastAttempt> getSessionsOfProject(int projectId, int pageSize, Optional<Long> lastId);

    List<StoredSessionWithLastAttempt> getSessionsOfWorkflowByName(int projectId, String workflowName, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttemptWithSession> getAttempts(boolean withRetriedAttempts, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttemptWithSession> getAttemptsOfProject(boolean withRetriedAttempts, int projectId, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttemptWithSession> getAttemptsOfWorkflow(boolean withRetriedAttempts, long workflowDefinitionId, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttempt> getAttemptsOfSession(long sessionId, int pageSize, Optional<Long> lastId);

    StoredSessionAttemptWithSession getAttemptById(long attemptId)
        throws ResourceNotFoundException;

    StoredSessionAttemptWithSession getLastAttemptByName(int projectId, String workflowName, Instant instant)
        throws ResourceNotFoundException;

    StoredSessionAttemptWithSession getAttemptByName(int projectId, String workflowName, Instant instant, String retryAttemptName)
        throws ResourceNotFoundException;

    List<StoredSessionAttemptWithSession> getOtherAttempts(long attemptId)
        throws ResourceNotFoundException;

    List<ArchivedTask> getTasksOfAttempt(long attemptId);

    long getActiveAttemptCount();

    interface SessionLockAction <T>
    {
        T call(SessionControlStore store, StoredSession storedSession)
            throws ResourceConflictException, ResourceNotFoundException;
    }

    <T> T putAndLockSession(Session session, SessionLockAction<T> func)
        throws ResourceConflictException, ResourceNotFoundException;
}
