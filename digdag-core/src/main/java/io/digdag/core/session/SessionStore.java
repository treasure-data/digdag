package io.digdag.core.session;

import java.util.List;
import com.google.common.base.*;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public interface SessionStore
{
    //List<StoredSession> getSessions(int pageSize, Optional<Long> lastId);

    //List<StoredSession> getSessionsOfRepository(int repositoryId, int pageSize, Optional<Long> lastId);

    //List<StoredSession> getSessionsOfWorkflow(long workflowId, int pageSize, Optional<Long> lastId);

    //StoredSession getSessionById(long sesId)
    //    throws ResourceNotFoundException;

    //StoredSession getSessionByName(String sesName)
    //    throws ResourceNotFoundException;

    //SessionStatusFlags getStatusFlags(long sesId)
    //    throws ResourceNotFoundException;

    List<StoredTask> getTasksOfAttempt(long aId, int pageSize, Optional<Long> lastId);

    interface SessionLockAction <T>
    {
        T call(SessionControlStore store, StoredSession storedSession)
            throws ResourceConflictException;
    }

    <T> T putAndLockSession(Session session, SessionLockAction<T> func)
        throws ResourceConflictException;
}
