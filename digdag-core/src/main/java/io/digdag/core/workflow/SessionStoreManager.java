package io.digdag.core.workflow;

import java.util.List;
import java.util.Date;
import com.google.common.base.*;

public interface SessionStoreManager
{
    SessionStore getSessionStore(int siteId);

    Date getStoreTime();

    boolean isAnyNotDoneWorkflows();

    // for WorkflowExecutorManager.enqueueReadyTasks
    List<Long> findAllReadyTaskIds(int maxEntries);

    // for WorkflowExecutorManager.IncrementalStatusPropagator.propagateStatus
    List<TaskStateSummary> findRecentlyChangedTasks(Date updatedSince, long lastId);

    // for WorkflowExecutorManager.propagateAllBlockedToReady
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

    // overload for SessionMonitorExecutor
    <T> Optional<T> lockRootTask(long sessionId, TaskLockActionWithDetails<T> func);

    interface SessionBuilderStore
    {
        <T> T addRootTask(Task task, TaskLockActionWithDetails<T> func);

        void addMonitors(long sessionId, List<SessionMonitor> monitors);
    }

    interface SessionBuilderAction
    {
        void call(StoredSession session, SessionBuilderStore store);
    }

    StoredSession newSession(Session newSession, SessionRelation relation, SessionBuilderAction func);

    SessionRelation getSessionRelationById(long sessionId);

    interface SessionMonitorAction
    {
        Optional<Date> schedule(StoredSessionMonitor monitor);
    }

    void lockReadySessionMonitors(Date currentTime, SessionMonitorAction func);
}
