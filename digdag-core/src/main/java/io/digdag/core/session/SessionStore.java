package io.digdag.core.session;

import java.util.List;
import java.time.Instant;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.spi.ac.AccessController;

public interface SessionStore
        extends SessionTransaction
{
    List<StoredSessionWithLastAttempt> getSessions(int pageSize, Optional<Long> lastId, int page, AccessController.ListFilter acFilter);

    int getSessionsCount(AccessController.ListFilter acFilter);

    StoredSessionWithLastAttempt getSessionById(long sessionId)
        throws ResourceNotFoundException;

    List<StoredSessionWithLastAttempt> getSessionsOfProject(int projectId, int pageSize, Optional<Long> lastId, int page, AccessController.ListFilter acFilter);

    int getSessionsCountOfProject(int projectId, AccessController.ListFilter acFilter);

    List<StoredSessionWithLastAttempt> getSessionsOfWorkflowByName(int projectId, String workflowName, int pageSize, Optional<Long> lastId, int page, AccessController.ListFilter acFilter);

    int getSessionsCountOfWorkflowByName(int projectId, String workflowName, AccessController.ListFilter acFilter);

    List<StoredSessionAttemptWithSession> getAttempts(boolean withRetriedAttempts, int pageSize, Optional<Long> lastId, AccessController.ListFilter acFilter);

    List<StoredSessionAttemptWithSession> getAttemptsOfProject(boolean withRetriedAttempts, int projectId, int pageSize, Optional<Long> lastId, AccessController.ListFilter acFilter);

    List<StoredSessionAttemptWithSession> getAttemptsOfWorkflow(boolean withRetriedAttempts, int projectId, String workflowName, int pageSize, Optional<Long> lastId, AccessController.ListFilter acFilter);

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
