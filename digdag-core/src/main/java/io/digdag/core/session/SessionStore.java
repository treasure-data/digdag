package io.digdag.core.session;

import java.util.List;
import java.time.Instant;
import com.google.common.base.*;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public interface SessionStore
{
    List<StoredSessionAttemptWithSession> getSessions(boolean withRetriedAttempts, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttemptWithSession> getSessionsOfRepository(boolean withRetriedAttempts, int repositoryId, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttemptWithSession> getSessionsOfWorkflow(boolean withRetriedAttempts, long workflowDefinitionId, int pageSize, Optional<Long> lastId);

    StoredSessionAttemptWithSession getSessionAttemptById(long attemptId)
        throws ResourceNotFoundException;

    StoredSessionAttemptWithSession getLastSessionAttemptByNames(int repositoryId, String workflowName, Instant instant)
        throws ResourceNotFoundException;

    StoredSessionAttemptWithSession getSessionAttemptByNames(int repositoryId, String workflowName, Instant instant, String retryAttemptName)
        throws ResourceNotFoundException;

    List<StoredSessionAttemptWithSession> getOtherAttempts(long attemptId)
        throws ResourceNotFoundException;

    List<StoredTask> getTasksOfAttempt(long attemptId);

    interface SessionLockAction <T>
    {
        T call(SessionControlStore store, StoredSession storedSession)
            throws ResourceConflictException;
    }

    <T> T putAndLockSession(Session session, SessionLockAction<T> func)
        throws ResourceConflictException;
}
