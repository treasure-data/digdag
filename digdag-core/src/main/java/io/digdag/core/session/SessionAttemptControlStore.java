package io.digdag.core.session;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.SessionStoreManager.TaskLockActionWithDetails;

public interface SessionAttemptControlStore
{
    int aggregateAndInsertTaskArchive(long attemptId);

    // for SessionMonitorExecutor to add monitor tasks
    public <T> T lockRootTask(long attemptId, TaskLockActionWithDetails<T> func) throws ResourceNotFoundException;

    int deleteAllTasksOfAttempt(long attemptId);

    boolean setDoneToAttemptState(long attemptId, boolean success);
}
