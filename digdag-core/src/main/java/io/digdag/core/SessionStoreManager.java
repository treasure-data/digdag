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























    /*
    // startable task means:
    //   * status code is in (BLOCKED, RETRY_WAITING, PLANNED_RETRY_WAITING)
    //   * and parent does not exist (root task) or parent is in (PLANNED, SUCCESS) state or (parent is in (RUN_ERROR, PLANNED_CHILD_ERROR, RETRY_WAITING, PLANNED_RETRY_WAITING) state and parent's ignore_parent_error option is true)
    //   * and all upstream tasks are in SUCCESS state
    List<StoredTask> findStartableTasks(Optional<Date> updatedSince, Date updatedUntil);

    // finishable task means:
    List<StoredTask> findFinishableTasks(Optional<Date> updatedSince, Date updatedUntil);

    //Optional<StoredTask> lockTaskAndParent(StoredTask task);

    //Optional<StoredTask> lockTaskAndChildren(StoredTask task);


    Optional<StoredTask> lockTask(long taskId, TaskStateCode code);


    boolean shortCircuitTransitionToPlanned(long taskId, TaskStateCode beforeCode);

    boolean transitionToReady(long taskId, TaskStateCode beforeCode);

    boolean transitionToSuccess(long taskId, TaskStateCode beforeCode);

    boolean transitionToPlanned(long taskId, TaskStateCode beforeCode, ConfigSource stateParams, TaskReport report);

    boolean propagateChildrenError(long taskId, TaskStateCode beforeCode, ConfigSource stateParams, ConfigSource error);

    boolean propagateChildrenErrorWithRetry(long taskId, TaskStateCode beforeCode, ConfigSource stateParams, ConfigSource error, int retryInterval);

    boolean taskError(long taskId, TaskStateCode beforeCode, ConfigSource stateParams, ConfigSource carryParams, ConfigSource error);

    boolean taskRetry(long taskId, TaskStateCode beforeCode, ConfigSource stateParams, ConfigSource carryParams, int retryInterval);

    boolean taskErrorWithRetry(long taskId, TaskStateCode beforeCode, ConfigSource stateParams, ConfigSource carryParams, ConfigSource error, int retryInterval);

    List<ConfigSource> getChildErrors(long parentTaskId);
    */
}
