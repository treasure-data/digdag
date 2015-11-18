package io.digdag.core.workflow;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.spi.TaskReport;
import io.digdag.core.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;

public class TaskControl
{
    private final TaskControlStore store;
    private final long id;
    private TaskStateCode state;

    public TaskControl(TaskControlStore store, long id, TaskStateCode state)
    {
        this.store = store;
        this.id = id;
        this.state = state;
        // TODO get TaskControlStore
    }

    public long getId()
    {
        return id;
    }

    public TaskStateCode getState()
    {
        return state;
    }

    public StoredTask addTasksExceptingRootTask(StoredTask rootTask, List<WorkflowTask> tasks)
    {
        return addTasks(rootTask, tasks, ImmutableList.of(), false, true);
    }

    public StoredTask addSubtasks(StoredTask parentTask, List<WorkflowTask> tasks,
            List<Long> rootUpstreamIds, boolean cancelSiblings)
    {
        return addTasks(parentTask, tasks, rootUpstreamIds, cancelSiblings, false);
    }

    private StoredTask addTasks(StoredTask parentTask, List<WorkflowTask> tasks,
            List<Long> rootUpstreamIds, boolean cancelSiblings, boolean firstTaskIsRootParentTask)
    {
        Preconditions.checkArgument(id == parentTask.getId());

        List<Long> indexToId = new ArrayList<>();
        List<String> indexToFullName = new ArrayList<>();

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
                indexToFullName.add(rootTask.getFullName());
                firstTask = false;
                continue;
            }

            String parentFullName = wt.getParentIndex()
                .transform(index -> indexToFullName.get(index))
                .or(parentTask.getFullName());
            String fullName = parentFullName + wt.getName();

            if (firstTask && cancelSiblings) {
                // TODO not implemented yet
            }

            Task task = Task.taskBuilder()
                .sessionId(parentTask.getSessionId())
                .parentId(Optional.of(
                            wt.getParentIndex()
                                .transform(index -> indexToId.get(index))
                                .or(parentTask.getId())
                            ))
                .fullName(fullName)
                .config(wt.getConfig())
                .taskType(wt.getTaskType())
                .state(TaskStateCode.BLOCKED)
                .build();

            long id = store.addSubtask(task);
            indexToId.add(id);
            indexToFullName.add(fullName);
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

    public boolean isAllChildrenDone()
    {
        return store.isAllChildrenDone(id);
    }

    public List<Config> collectChildrenErrors()
    {
        return store.collectChildrenErrors(id);
    }

    public boolean setReadyToRunning()
    {
        // TODO checkState
        if (store.setState(id, state, TaskStateCode.RUNNING)) {
            state = TaskStateCode.RUNNING;
            return true;
        }
        return false;
    }

    // all necessary information is already set by setRunningToPlanned. Here simply set state to SUCCESS
    public boolean setPlannedToSuccess()
    {
        // TODO checkState
        if (store.setState(id, state, TaskStateCode.SUCCESS)) {
            state = TaskStateCode.SUCCESS;
            return true;
        }
        return false;
    }

    public boolean setPlannedToError(Config stateParams, Config error)
    {
        // TODO checkState
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.ERROR;
            return true;
        }
        return false;
    }

    public boolean setRunningToShortCircuitError(Config stateParams, Config error)
    {
        // TODO checkState
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.ERROR;
            return true;
        }
        return false;
    }

    public boolean setPlannedToPlanned(Config stateParams, Config error)
    {
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.PLANNED, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
    }

    public boolean setPlannedToGroupError(Config stateParams, Config error)
    {
        // TODO checkState
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.GROUP_ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.GROUP_ERROR;
            return true;
        }
        return false;
    }

    // group retry
    public boolean setPlannedToGroupRetry(Config stateParams, int retryInterval)
    {
        // TODO checkState
        if (store.setStateWithStateParamsUpdate(id, state, TaskStateCode.GROUP_RETRY_WAITING, stateParams, Optional.of(retryInterval))) {
            state = TaskStateCode.GROUP_RETRY_WAITING;
        }
        return false;
        // propagateChildrenErrorWithRetry
    }

    // collect parameters and set them to ready tasks at the same time? no, because children's carry_params are not propagated to parents
    public int trySetChildrenBlockedToReadyOrShortCircuitPlanned()
    {
        // TODO checkState
        return store.trySetChildrenBlockedToReadyOrShortCircuitPlanned(id);
    }

    //// trySetChildrenBlockedToReadyOrShortCircuitPlanned for root tasks
    //public boolean setRootPlannedToReady()
    //{
    //    // TODO checkState
    //    return store.trySetBlockedToReadyOrShortCircuitPlanned(id);
    //    // TODO set state
    //}

    ////
    // for taskFinished callback
    //

    // to planned with successful report
    public boolean setRunningToPlanned(Config stateParams, TaskReport report)
    {
        if (store.setStateWithSuccessDetails(id, state, TaskStateCode.PLANNED, stateParams, report)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
        // transitionToPlanned
    }

    // to planned with error
    public boolean setRunningToPlanned(Config stateParams, Config error)
    {
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.PLANNED, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
    }

    // to retry with error
    public boolean setRunningToRetry(Config stateParams, Config error, int retryInterval)
    {
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.RETRY_WAITING, stateParams, Optional.of(retryInterval), error)) {
            state = TaskStateCode.RETRY_WAITING;
            return true;
        }
        return false;
    }

    // to retry without error
    public boolean setRunningToRetry(Config stateParams, int retryInterval)
    {
        if (store.setStateWithStateParamsUpdate(id, state, TaskStateCode.RETRY_WAITING, stateParams, Optional.absent())) {
            state = TaskStateCode.RETRY_WAITING;
            return true;
        }
        return false;
    }


    ////
    // for control interface
    //
    //public void setAnyToCanceled();
}
