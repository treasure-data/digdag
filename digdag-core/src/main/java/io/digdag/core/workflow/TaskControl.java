package io.digdag.core.workflow;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.Limits;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.Task;
import io.digdag.core.session.TaskControlStore;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskStateFlags;
import io.digdag.spi.TaskResult;
import io.digdag.client.config.Config;

public class TaskControl
{
    private final TaskControlStore store;
    private final StoredTask task;
    private TaskStateCode state;

    public TaskControl(TaskControlStore store, StoredTask task)
    {
        this.store = store;
        this.task = task;
        this.state = task.getState();
    }

    public StoredTask get()
    {
        return task;
    }

    public long getId()
    {
        return task.getId();
    }

    public TaskStateCode getState()
    {
        return state;
    }

    public static long addInitialTasksExceptingRootTask(
            TaskControlStore store, long attemptId, long rootTaskId,
            WorkflowTaskList tasks)
    {
        return addTasks(store, attemptId, rootTaskId,
                tasks, ImmutableList.of(),
                false, true, true);
    }

    public long addGeneratedSubtasks(WorkflowTaskList tasks,
            List<Long> rootUpstreamIds, boolean cancelSiblings)
    {
        return addTasks(store, task.getAttemptId(), task.getId(),
                tasks, rootUpstreamIds,
                cancelSiblings, false, false);
    }

    private static long addTasks(TaskControlStore store,
            long attemptId, long parentTaskId, WorkflowTaskList tasks, List<Long> rootUpstreamIds,
            boolean cancelSiblings, boolean firstTaskIsRootStoredParentTask, boolean isInitialTask)
    {
        List<Long> indexToId = new ArrayList<>();

        Long rootTaskId;
        if (firstTaskIsRootStoredParentTask) {
            // tasks.get(0) == parentTask == root task
            rootTaskId = parentTaskId;
        }
        else {
            rootTaskId = null;
        }

        boolean firstTask = true;
        for (WorkflowTask wt : tasks) {

            // Limit the total number of tasks in a session.
            // Note: This is racy, so perform this check for each task being added to make it less likely that
            //       concurrent task addition will result in the limit being greatly exceeded.
            long taskCount = store.getTaskCount(attemptId);
            if (taskCount >= Limits.maxWorkflowTasks()) {
                throw new TaskLimitExceededException("Too many tasks. Limit: " + Limits.maxWorkflowTasks() + ", Current: " + taskCount);
            }

            if (firstTask && firstTaskIsRootStoredParentTask) {
                indexToId.add(rootTaskId);
                firstTask = false;
                continue;  // skip storing this task because (tasks.get(0) == parentTaskId == root task) is already stored as parentTaskId
            }

            if (firstTask && cancelSiblings) {
                // TODO not implemented yet
            }

            Task task = Task.taskBuilder()
                .parentId(Optional.of(
                            wt.getParentIndex()
                                .transform(index -> indexToId.get(index))
                                .or(parentTaskId)
                            ))
                .fullName(wt.getFullName())
                .config(TaskConfig.validate(wt.getConfig()))
                .taskType(wt.getTaskType())
                .state(TaskStateCode.BLOCKED)
                .stateFlags(isInitialTask ? TaskStateFlags.empty().withInitialTask() : TaskStateFlags.empty())
                .build();

            long id = store.addSubtask(attemptId, task);
            indexToId.add(id);
            if (!wt.getUpstreamIndexes().isEmpty()) {
                store.addDependencies(
                        id,
                        wt.getUpstreamIndexes()
                            .stream()
                            .map(index -> indexToId.get(index))
                            .collect(Collectors.toList())
                        );
            }

            if (firstTask) {
                // the root task was stored right now.
                store.addDependencies(id, rootUpstreamIds);
                rootTaskId = id;
            }
            firstTask = false;
        }

        return rootTaskId;
    }

    ////
    // for state propagation logic of WorkflowExecutorManager
    //

    public boolean isAnyProgressibleChild()
    {
        return store.isAnyProgressibleChild(getId());
    }

    public boolean isAnyErrorChild()
    {
        return store.isAnyErrorChild(getId());
    }

    public List<Config> collectChildrenErrors()
    {
        return store.collectChildrenErrors(getId());
    }

    public boolean setReadyToRunning()
    {
        if (store.setState(getId(), TaskStateCode.READY, TaskStateCode.RUNNING)) {
            state = TaskStateCode.RUNNING;
            return true;
        }
        return false;
    }

    public boolean setToCanceled()
    {
        if (store.setDoneState(getId(), state, TaskStateCode.CANCELED)) {
            state = TaskStateCode.CANCELED;
            return true;
        }
        return false;
    }

    // all necessary information is already set by setRunningToSuccessfulPlanned. Here simply set state to SUCCESS
    public boolean setPlannedToSuccess()
    {
        if (store.setDoneState(getId(), TaskStateCode.PLANNED, TaskStateCode.SUCCESS)) {
            state = TaskStateCode.SUCCESS;
            return true;
        }
        return false;
    }

    public boolean setPlannedToError()
    {
        if (store.setDoneState(getId(), TaskStateCode.PLANNED, TaskStateCode.ERROR)) {
            state = TaskStateCode.ERROR;
            return true;
        }
        return false;
    }

    public boolean setRunningToShortCircuitError(Config error)
    {
        if (store.setDoneStateShortCircuit(getId(), TaskStateCode.RUNNING, TaskStateCode.ERROR, error)) {
            state = TaskStateCode.ERROR;
            return true;
        }
        return false;
    }

    public boolean setPlannedToPlannedWithDelayedGroupError()
    {
        if (store.setPlannedStateWithDelayedError(getId(), TaskStateCode.PLANNED, TaskStateCode.PLANNED, TaskStateFlags.DELAYED_GROUP_ERROR, Optional.absent())) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
    }

    public boolean setPlannedToGroupError()
    {
        if (store.setDoneState(getId(), TaskStateCode.PLANNED, TaskStateCode.GROUP_ERROR)) {
            state = TaskStateCode.GROUP_ERROR;
            return true;
        }
        return false;
    }

    // to group retry (group retry is always without error)
    public boolean setPlannedToGroupRetryWaiting(Config stateParams, int retryInterval)
    {
        if (store.setRetryWaitingState(getId(), TaskStateCode.PLANNED, TaskStateCode.GROUP_RETRY_WAITING, retryInterval, stateParams, Optional.absent())) {
            state = TaskStateCode.GROUP_RETRY_WAITING;
            return true;
        }
        return false;
        // propagateChildrenErrorWithRetry
    }

    public boolean copyInitialTasksForRetry(List<Long> recursiveChildrenIdList)
    {
        return store.copyInitialTasksForRetry(recursiveChildrenIdList);
    }

    public boolean setGroupRetryReadyToPlanned()
    {
        if (store.setPlannedStateSuccessful(getId(), TaskStateCode.READY, TaskStateCode.PLANNED, TaskResult.empty(task.getStateParams().getFactory()))) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
        // transitionToPlanned
    }

    ////
    // for taskFinished callback
    //

    // to planned with successful report
    public boolean setRunningToPlannedSuccessful(TaskResult result)
    {
        if (store.setPlannedStateSuccessful(getId(), TaskStateCode.RUNNING, TaskStateCode.PLANNED, result)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
        // transitionToPlanned
    }

    // to planned with error
    public boolean setRunningToPlannedWithDelayedError(Config error)
    {
        if (store.setPlannedStateWithDelayedError(getId(), TaskStateCode.RUNNING, TaskStateCode.PLANNED, TaskStateFlags.DELAYED_ERROR, Optional.of(error))) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
    }

    // to retry with error
    public boolean setRunningToRetryWaiting(Config stateParams, int retryInterval, Config error)
    {
        if (store.setRetryWaitingState(getId(), TaskStateCode.RUNNING, TaskStateCode.RETRY_WAITING, retryInterval, stateParams, Optional.of(error))) {
            state = TaskStateCode.RETRY_WAITING;
            return true;
        }
        return false;
    }

    // to retry without error
    public boolean setRunningToRetryWaiting(Config stateParams, int retryInterval)
    {
        if (store.setRetryWaitingState(getId(), TaskStateCode.RUNNING, TaskStateCode.RETRY_WAITING, retryInterval, stateParams, Optional.absent())) {
            state = TaskStateCode.RETRY_WAITING;
            return true;
        }
        return false;
    }
}
