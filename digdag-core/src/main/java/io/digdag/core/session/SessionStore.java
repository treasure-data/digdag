package io.digdag.core.session;

import java.util.List;
import java.time.Instant;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public interface SessionStore
        extends SessionTransaction
{
    List<StoredSessionWithLastAttempt> getSessions(int pageSize, Optional<Long> lastId);

    List<StoredSessionWithLastAttempt> getSessions(int pageSize, Optional<Long> lastId, int pageNumber);

    Integer getTotalSessionsCount(Optional<Long> lastId);

    Integer getTotalProjectSessionsCount(Optional<Long> lastId, int projectId);

    StoredSessionWithLastAttempt getSessionById(long sessionId)
        throws ResourceNotFoundException;

    List<StoredSessionWithLastAttempt> getSessionsOfProject(int projectId, int pageSize, Optional<Long> lastId);

    List<StoredSessionWithLastAttempt> getSessionsOfProject(int projectId, int pageSize, Optional<Long> lastId, int pageNumber);

    List<StoredSessionWithLastAttempt> getSessionsOfWorkflowByName(int projectId, String workflowName, int pageSize, Optional<Long> lastId);

    List<StoredSessionWithLastAttempt> getSessionsOfWorkflowByName(int projectId, String workflowName, int pageSize, Optional<Long> lastId, int pageNumber);

    List<StoredSessionAttemptWithSession> getAttempts(boolean withRetriedAttempts, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttemptWithSession> getAttemptsOfProject(boolean withRetriedAttempts, int projectId, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttemptWithSession> getAttemptsOfWorkflow(boolean withRetriedAttempts, int projectId, String workflowName, int pageSize, Optional<Long> lastId);

    List<StoredSessionAttemptWithSession> getActiveAttemptsOfWorkflow(int projectId, String workflowName, int pageSize, Optional<Long> lastId);

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

    interface SessionTransactionAction <T>
    {
        T call(SessionTransaction transaction)
            throws Exception;
    }

    <T> T sessionTransaction(SessionTransactionAction<T> func)
            throws Exception;
}
