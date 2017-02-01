package io.digdag.core.session;

import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import java.time.Instant;

public interface SessionTransaction
{
    interface SessionLockAction <T>
    {
        T call(SessionControlStore store, StoredSession storedSession)
            throws ResourceConflictException, ResourceNotFoundException;
    }

    <T> T putAndLockSession(Session session, SessionLockAction<T> func)
        throws ResourceConflictException, ResourceNotFoundException;

    long getActiveAttemptCount();

    Optional<Instant> getLastExecutedSessionTime(
            int projectId, String workflowName,
            Instant beforeThisSessionTime);
}
