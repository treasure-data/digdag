package io.digdag.core.session;

import java.util.List;
import java.util.Date;
import com.google.common.base.*;
import io.digdag.core.workflow.TaskControl;
import io.digdag.spi.RevisionInfo;
import io.digdag.spi.config.Config;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public interface SessionStoreManager
{
    SessionStore getSessionStore(int siteId);

    Date getStoreTime();

    // for WorkflowExecutorManager.runUntilAny
    boolean isAnyNotDoneWorkflows();

    // for WorkflowExecutorManager.enqueueReadyTasks
    List<Long> findAllReadyTaskIds(int maxEntries);

    // for WorkflowExecutorManager.enqueueTask
    StoredSession getSessionById(long sesId)
        throws ResourceNotFoundException;

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
    <T> Optional<T> lockTaskIfExists(long taskId, TaskLockAction<T> func);

    // overload for taskFinished
    <T> Optional<T> lockTaskIfExists(long taskId, TaskLockActionWithDetails<T> func);

    // overload for SessionMonitorExecutor
    <T> Optional<T> lockRootTaskIfExists(long sessionId, TaskLockActionWithDetails<T> func);

    interface SessionBuilderStore
    {
        <T> T addRootTask(Task task, TaskLockActionWithDetails<T> func);

        void addMonitors(long sessionId, List<SessionMonitor> monitors);
    }

    interface SessionBuilderAction
    {
        void call(StoredSession session, SessionBuilderStore store);
    }

    StoredSession newSession(int siteId, Session newSession, Optional<SessionRelation> relation, SessionBuilderAction func)
        throws ResourceConflictException;

    Optional<RevisionInfo> getAssociatedRevisionInfo(long sesId);

    interface SessionMonitorAction
    {
        Optional<Date> schedule(StoredSessionMonitor monitor);
    }

    void lockReadySessionMonitors(Date currentTime, SessionMonitorAction func);

    List<TaskRelation> getTaskRelations(long sesId);

    List<Config> getExportParams(List<Long> idList);

    List<Config> getCarryParams(List<Long> idList);
}
