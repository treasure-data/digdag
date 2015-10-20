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

    public interface TaskLockAction <T>
    {
        public T call(TaskControl lockedTask);
    }

    public interface TaskLockActionWithDetails <T>
    {
        public T call(TaskControl lockedTask, StoredTask task);
    }

    // overload for polling
    <T> Optional<T> lockTask(long taskId, TaskLockAction<T> func);

    // overload for taskFinished
    <T> Optional<T> lockTask(long taskId, TaskLockActionWithDetails<T> func);

    public interface SessionBuilderStore
    {
        <T> T addRootTask(Task task, TaskLockActionWithDetails<T> func);
    }

    public interface SessionBuilderAction
    {
        public void call(StoredSession session, SessionBuilderStore store);
    }

    StoredSession newSession(int siteId, Session newSession, SessionRelation relation, SessionBuilderAction func);
}
