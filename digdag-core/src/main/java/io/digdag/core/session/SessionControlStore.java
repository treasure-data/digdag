package io.digdag.core.session;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public interface SessionControlStore
{
    StoredSessionAttempt insertAttempt(long sessionId, int repoId, SessionAttempt attempt)
        throws ResourceConflictException;

    StoredSessionAttempt getLastAttempt(long sessionId)
        throws ResourceNotFoundException;

    interface SessionBuilderAction <T>
    {
        T call(TaskControlStore store, StoredTask task);
    }

    <T> T insertRootTask(long attemptId, Task task, SessionBuilderAction<T> func);

    void insertMonitors(long attemptId, List<SessionMonitor> monitors);
}
