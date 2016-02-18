package io.digdag.core.workflow;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.TaskStateSummary;
import io.digdag.core.session.Task;
import io.digdag.core.session.TaskControlStore;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskStateFlags;
import io.digdag.spi.TaskResult;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.workflow.TaskConfig;
import io.digdag.core.workflow.WorkflowTask;
import io.digdag.core.workflow.WorkflowTaskList;
import static com.google.common.base.Preconditions.checkState;

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

    public StoredTask addTasksExceptingRootTask(WorkflowTaskList tasks)
    {
        return addTasks(task, tasks, ImmutableList.of(), false, true);
    }

    public StoredTask addSubtasks(WorkflowTaskList tasks,
            List<Long> rootUpstreamIds, boolean cancelSiblings)
    {
        return addTasks(task, tasks, rootUpstreamIds, cancelSiblings, false);
    }

    private StoredTask addTasks(StoredTask parentTask, WorkflowTaskList tasks,
            List<Long> rootUpstreamIds, boolean cancelSiblings, boolean firstTaskIsRootStoredParentTask)
    {
        Preconditions.checkArgument(getId() == parentTask.getId());

        List<Long> indexToId = new ArrayList<>();

        StoredTask rootTask;
        if (firstTaskIsRootStoredParentTask) {
            // tasks.get(0) == parentTask == root task
            rootTask = parentTask;
        }
        else {
            rootTask = null;
        }

        boolean firstTask = true;
        for (WorkflowTask wt : tasks) {
            if (firstTask && firstTaskIsRootStoredParentTask) {
                indexToId.add(rootTask.getId());
                firstTask = false;
                continue;  // skip storing this task because (tasks.get(0) == parentTask == root task) is already stored as parentTask
            }

            if (firstTask && cancelSiblings) {
                // TODO not implemented yet
            }

            Task task = Task.taskBuilder()
                .parentId(Optional.of(
                            wt.getParentIndex()
                                .transform(index -> indexToId.get(index))
                                .or(parentTask.getId())
                            ))
                .fullName(wt.getFullName())
                .config(TaskConfig.validate(wt.getConfig()))
                .taskType(wt.getTaskType())
                .state(TaskStateCode.BLOCKED)
                .build();

            long id = store.addSubtask(parentTask.getAttemptId(), task);
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
                try {
                    rootTask = store.getTaskById(id);
                }
                catch (ResourceNotFoundException ex) {
                    throw new IllegalStateException("Database state error", ex);
                }
                firstTask = false;
            }
        }

        return rootTask;
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
        }
        return false;
        // propagateChildrenErrorWithRetry
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
