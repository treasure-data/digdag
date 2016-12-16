package io.digdag.core.session;

import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import java.time.Instant;

public interface DelayedAttemptControlStore
{
    interface DelayedSessionLockAction <T>
    {
        T call(SessionControlStore store, StoredSessionAttemptWithSession storedAttempt)
            throws ResourceConflictException, ResourceNotFoundException;
    }

    <T> T lockSessionOfAttempt(long attemptId, DelayedSessionLockAction<T> func)
        throws ResourceConflictException, ResourceNotFoundException;

    void delayDelayedAttempt(long attemptId, Instant nextRunTime);

    void completeDelayedAttempt(long attemptId);
}
