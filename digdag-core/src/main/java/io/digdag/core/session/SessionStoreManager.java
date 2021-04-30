package io.digdag.core.session;

import java.util.List;
import java.time.Instant;
import com.google.common.base.*;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;

public interface SessionStoreManager
{
    SessionStore getSessionStore(int siteId);

    Instant getStoreTime();

    // for WorkflowExecutor.enqueueTask
    int getSiteIdOfTask(long taskId)
        throws ResourceNotFoundException;

    StoredSessionAttemptWithSession getAttemptWithSessionById(long attemptId)
        throws ResourceNotFoundException;

    // for WorkflowExecutor.runUntilDone
    AttemptStateFlags getAttemptStateFlags(long attemptId)
        throws ResourceNotFoundException;

    // for WorkflowExecutor.runUntilAny
    boolean isAnyNotDoneAttempts();

    // for WorkflowExecutor.enqueueReadyTasks (Keep for compatibility)
    default List<Long> findAllReadyTaskIds(int maxEntries) { return findAllReadyTaskIds(maxEntries, false); }

    /**
     * for WorkflowExecutor.enqueueReadyTasks
     * @param maxEntries  max number to fetch
     * @param randomFetch fetch randomly or not(original behavior)
     * @return
     */
    List<Long> findAllReadyTaskIds(int maxEntries, boolean randomFetch);


    // for AttemptTimeoutEnforcer.enforceAttemptTTLs
    List<StoredSessionAttempt> findActiveAttemptsCreatedBefore(Instant createdBefore, long lastId, int limit);

    // for profiling
    List<StoredSessionAttemptWithSession> findFinishedAttemptsWithSessions(Instant createdFrom, Instant createdTo, long lastId, int limit);

    // for AttemptTimeoutEnforcer.enforceTaskTTLs
    List<TaskAttemptSummary> findTasksStartedBeforeWithState(TaskStateCode[] states, Instant startedBefore, long lastId, int limit);

    interface AttemptLockAction <T>
    {
        T call(SessionAttemptControlStore store, SessionAttemptSummary summary);
    }

    // for WorkflowExecutor.propagateSessionArchive
    <T> Optional<T> lockAttemptIfExists(long attemptId, AttemptLockAction<T> func);

    // for WorkflowExecutorManager.IncrementalStatusPropagator.propagateStatus
    List<TaskStateSummary> findRecentlyChangedTasks(Instant updatedSince, long lastId);

    // for WorkflowExecutorManager.propagateAllPlannedToDone
    List<Long> findTasksByState(TaskStateCode state, long lastId);

    // for WorkflowExecutorManager.propagateSessionArchive
    List<TaskAttemptSummary> findRootTasksByStates(TaskStateCode[] states, long lastId);

    // for WorkflowExecutorManager.propagateBlockedChildrenToReady
    List<Long> findDirectParentsOfBlockedTasks(long lastId);

    boolean requestCancelAttempt(long attemptId);

    int trySetRetryWaitingToReady();

    interface TaskLockAction <T>
    {
        T call(TaskControlStore lockedTask);
    }

    interface TaskLockActionWithDetails <T>
    {
        T call(TaskControlStore lockedTask, StoredTask storedTask);
    }

    <T> Optional<T> lockTaskIfExists(long taskId, TaskLockAction<T> func);

    <T> Optional<T> lockTaskIfNotLocked(long taskId, TaskLockAction<T> func);

    <T> Optional<T> lockTaskIfExists(long taskId, TaskLockActionWithDetails<T> func);

    <T> Optional<T> lockTaskIfNotLocked(long taskId, TaskLockActionWithDetails<T> func);

    interface SessionMonitorAction
    {
        // returns next run time
        Optional<Instant> schedule(StoredSessionMonitor monitor);
    }

    void lockReadySessionMonitors(Instant currentTime, SessionMonitorAction func);

    List<TaskRelation> getTaskRelations(long attemptId);

    List<Config> getExportParams(List<Long> idList);

    List<ParameterUpdate> getStoreParams(List<Long> idList);

    List<Config> getErrors(List<Long> idList);

    interface DelayedAttemptAction
    {
        void submit(DelayedAttemptControlStore lockedAttempt, StoredDelayedSessionAttempt locked);
    }

    void lockReadyDelayedAttempts(Instant currentTime, DelayedAttemptAction func);
}
