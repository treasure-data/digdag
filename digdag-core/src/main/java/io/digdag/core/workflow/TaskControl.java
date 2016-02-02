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
import io.digdag.spi.TaskReport;
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
            List<Long> rootUpstreamIds, boolean cancelSiblings, boolean firstTaskIsRootParentTask)
    {
        Preconditions.checkArgument(getId() == parentTask.getId());

        List<Long> indexToId = new ArrayList<>();

        StoredTask rootTask;
        if (firstTaskIsRootParentTask) {
            // tasks.get(0) == parentTask == root task
            rootTask = parentTask;
        }
        else {
            rootTask = null;
        }

        boolean firstTask = true;
        for (WorkflowTask wt : tasks) {
            if (firstTask && firstTaskIsRootParentTask) {
                indexToId.add(rootTask.getId());
                firstTask = false;
                continue;
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
                // this is root task
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
        if (store.setState(getId(), state, TaskStateCode.CANCELED)) {
            state = TaskStateCode.CANCELED;
            return true;
        }
        return false;
    }

    // all necessary information is already set by setRunningToPlanned. Here simply set state to SUCCESS
    public boolean setPlannedToSuccess()
    {
        if (store.setState(getId(), TaskStateCode.PLANNED, TaskStateCode.SUCCESS)) {
            state = TaskStateCode.SUCCESS;
            return true;
        }
        return false;
    }

    public boolean setPlannedToError(Config stateParams, Config error)
    {
        if (store.setStateWithErrorDetails(getId(), TaskStateCode.PLANNED, TaskStateCode.ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.ERROR;
            return true;
        }
        return false;
    }

    public boolean setRunningToShortCircuitError(Config stateParams, Config error)
    {
        if (store.setStateWithErrorDetails(getId(), TaskStateCode.RUNNING, TaskStateCode.ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.ERROR;
            return true;
        }
        return false;
    }

    public boolean setPlannedToPlanned(Config stateParams, Config error)
    {
        if (store.setStateWithErrorDetails(getId(), TaskStateCode.PLANNED, TaskStateCode.PLANNED, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
    }

    public boolean setPlannedToGroupError(Config stateParams, Config error)
    {
        if (store.setStateWithErrorDetails(getId(), TaskStateCode.PLANNED, TaskStateCode.GROUP_ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.GROUP_ERROR;
            return true;
        }
        return false;
    }

    // group retry
    public boolean setPlannedToGroupRetry(Config stateParams, int retryInterval)
    {
        if (store.setStateWithStateParamsUpdate(getId(), TaskStateCode.PLANNED, TaskStateCode.GROUP_RETRY_WAITING, stateParams, Optional.of(retryInterval))) {
            state = TaskStateCode.GROUP_RETRY_WAITING;
        }
        return false;
        // propagateChildrenErrorWithRetry
    }

    ////
    // for taskFinished callback
    //

    // to planned with successful report
    public boolean setRunningToPlanned(Config stateParams, TaskReport report)
    {
        if (store.setStateWithSuccessDetails(getId(), state, TaskStateCode.PLANNED, stateParams, report)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
        // transitionToPlanned
    }

    // to planned with error
    public boolean setRunningToPlanned(Config stateParams, Config error)
    {
        if (store.setStateWithErrorDetails(getId(), state, TaskStateCode.PLANNED, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
    }

    // to retry with error
    public boolean setRunningToRetry(Config stateParams, Config error, int retryInterval)
    {
        if (store.setStateWithErrorDetails(getId(), TaskStateCode.RUNNING, TaskStateCode.RETRY_WAITING, stateParams, Optional.of(retryInterval), error)) {
            state = TaskStateCode.RETRY_WAITING;
            return true;
        }
        return false;
    }

    // to retry without error
    public boolean setRunningToRetry(Config stateParams, int retryInterval)
    {
        if (store.setStateWithStateParamsUpdate(getId(), TaskStateCode.RUNNING, TaskStateCode.RETRY_WAITING, stateParams, Optional.of(retryInterval))) {
            state = TaskStateCode.RETRY_WAITING;
            return true;
        }
        return false;
    }
}
