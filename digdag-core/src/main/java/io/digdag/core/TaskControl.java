package io.digdag.core;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.stream.Collectors;
import com.google.common.base.*;
import com.google.common.collect.*;

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

    public StoredTask addSubtasks(StoredTask thisTask, List<WorkflowTask> tasks, int indexOffset)
    {
        return addSubtasks(thisTask, tasks, ImmutableList.of(), false, indexOffset);
    }

    public StoredTask addSubtasks(StoredTask thisTask, List<WorkflowTask> tasks,
            List<Long> rootUpstreamIds, boolean cancelSiblings, int indexOffset)
    {
        Preconditions.checkArgument(id == thisTask.getId());

        List<Long> indexToId = new ArrayList<>();
        List<String> indexToFullName = new ArrayList<>();
        indexToId.add(thisTask.getId());
        indexToFullName.add(thisTask.getFullName());

        StoredTask rootTask = null;
        System.out.println("Adding tasks: "+tasks);
        for (WorkflowTask wt : tasks) {
            String parentFullName = wt.getParentIndex()
                .transform(index -> indexToFullName.get(index + indexOffset))
                .or(thisTask.getFullName());
            String fullName = parentFullName + wt.getName();

            if (rootTask == null) {
                // this is root task
                if (cancelSiblings) {
                    // TODO not implemented yet
                }
            }

            Task task = Task.taskBuilder()
                .sessionId(thisTask.getSessionId())
                .parentId(Optional.of(
                            wt.getParentIndex()
                                .transform(index -> indexToId.get(index + indexOffset))
                                .or(thisTask.getId())
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
                            .map(index -> indexToId.get(index + indexOffset))
                            .collect(Collectors.toList())
                        );
            }

            if (rootTask == null) {
                // this is root task
                store.addDependencies(id, rootUpstreamIds);
                rootTask = store.getTaskById(id);
            }
        }

        return rootTask;
    }

    ////
    // for state propagation logic of SessionExecutorManager
    //

    public boolean isAllChildrenDone()
    {
        return store.isAllChildrenDone(id);
    }

    public List<ConfigSource> collectChildrenErrors()
    {
        return store.collectChildrenErrors(id);
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

    public boolean setPlannedToError(ConfigSource stateParams, ConfigSource error)
    {
        // TODO checkState
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.ERROR;
            return true;
        }
        return false;
    }

    public boolean setRunningToShortCircuitError(ConfigSource stateParams, ConfigSource error)
    {
        // TODO checkState
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.ERROR;
            return true;
        }
        return false;
    }

    public boolean setPlannedToPlanned(ConfigSource stateParams, ConfigSource error)
    {
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.PLANNED, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
    }

    public boolean setPlannedToGroupError(ConfigSource stateParams, ConfigSource error)
    {
        // TODO checkState
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.GROUP_ERROR, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.GROUP_ERROR;
            return true;
        }
        return false;
    }

    // group retry
    public boolean setPlannedToGroupRetry(ConfigSource stateParams, int retryInterval)
    {
        // TODO checkState
        if (store.setStateWithStateParamsUpdate(id, state, TaskStateCode.GROUP_RETRY_WAITING, stateParams, Optional.of(retryInterval))) {
            state = TaskStateCode.GROUP_RETRY_WAITING;
        }
        return false;
        // propagateChildrenErrorWithRetry
    }

    // collect parameters and set them to ready tasks at the same time? no, because children's carry_params are not propagated to parents
    public long trySetChildrenBlockedToReadyOrShortCircuitPlanned()
    {
        // TODO checkState
        return store.trySetChildrenBlockedToReadyOrShortCircuitPlanned(id);
    }

    // trySetChildrenBlockedToReadyOrShortCircuitPlanned for root tasks
    public boolean setRootPlannedToReady()
    {
        // TODO checkState
        if (store.setState(id, state, TaskStateCode.READY)) {
            state = TaskStateCode.READY;
            return true;
        }
        return false;
    }

    ////
    // for taskFinished callback
    //

    // to planned with successful report
    public boolean setRunningToPlanned(ConfigSource stateParams, ConfigSource carryParams, TaskReport report)
    {
        if (store.setStateWithSuccessDetails(id, state, TaskStateCode.PLANNED, stateParams, carryParams, report)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
        // transitionToPlanned
    }

    // to planned with error
    public boolean setRunningToPlanned(ConfigSource stateParams, ConfigSource error)
    {
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.PLANNED, stateParams, Optional.absent(), error)) {
            state = TaskStateCode.PLANNED;
            return true;
        }
        return false;
    }

    // to retry with error
    public boolean setRunningToRetry(ConfigSource stateParams, ConfigSource error, int retryInterval)
    {
        if (store.setStateWithErrorDetails(id, state, TaskStateCode.RETRY_WAITING, stateParams, Optional.of(retryInterval), error)) {
            state = TaskStateCode.RETRY_WAITING;
            return true;
        }
        return false;
    }

    // to retry without error
    public boolean setRunningToRetry(ConfigSource stateParams, int retryInterval)
    {
        if (store.setStateWithStateParamsUpdate(id, state, TaskStateCode.RETRY_WAITING, stateParams, Optional.absent())) {
            state = TaskStateCode.RETRY_WAITING;
            return true;
        }
        return false;
    }

    public StoredTask addTasks(List<WorkflowTask> tasks, List<Long> rootUpstreamIds, TaskErrorMode rootErrorMode, boolean cancelSiblings)
    {
        // TODO
        return null;
    }


    ////
    // for control interface
    //
    //public void setAnyToCanceled();
}
