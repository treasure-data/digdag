package io.digdag.core;

import java.util.List;
import java.util.Date;
import com.google.common.base.*;

public interface SessionStoreManager
{
    SessionStore getSessionStore(int siteId);

    List<StoredTask> getAllTasks();  // TODO only for testing

    Date getStoreTime();

    boolean isAnyNotDoneWorkflows();

    // for SessionExecutorManager.enqueueReadyTasks
    List<Long> findAllReadyTaskIds(int maxEntries);

    // for SessionExecutorManager.IncrementalStatusPropagator.propagateStatus
    List<TaskStateSummary> findRecentlyChangedTasks(Date updatedSince, long lastId);

    // for SessionExecutorManager.propagateAllBlockedToReady
    List<TaskStateSummary> findTasksByState(TaskStateCode state, long lastId);

    int trySetRetryWaitingToReady();

    interface TaskLockAction <T>
    {
        T call(TaskControl lockedTask);
    }

    interface TaskLockActionWithDetails <T>
    {
        T call(TaskControl lockedTask, StoredTask task);
    }

    // overload for polling
    <T> Optional<T> lockTask(long taskId, TaskLockAction<T> func);

    // overload for taskFinished
    <T> Optional<T> lockTask(long taskId, TaskLockActionWithDetails<T> func);

    interface SessionBuilderStore
    {
        <T> T addRootTask(Task task, TaskLockActionWithDetails<T> func);
    }

    interface SessionBuilderAction
    {
        void call(StoredSession session, SessionBuilderStore store);
    }

    StoredSession newSession(int siteId, Session newSession, SessionNamespace namespace, SessionBuilderAction func);
}
