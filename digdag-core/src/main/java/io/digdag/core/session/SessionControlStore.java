package io.digdag.core.session;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceConflictException;

public interface SessionControlStore
{
    Optional<StoredSessionAttempt> tryLastAttempt(long sessionId);

    StoredSessionAttempt insertAttempt(long sessionId, int repoId, SessionAttempt attempt)
        throws ResourceConflictException;

    interface SessionBuilderAction <T>
    {
        T call(TaskControlStore store, StoredTask task);
    }

    <T> T insertRootTask(long attemptId, Task task, SessionBuilderAction<T> func);

    void insertMonitors(long attemptId, List<SessionMonitor> monitors);
}
