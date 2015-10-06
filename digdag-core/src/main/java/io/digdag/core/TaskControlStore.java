package io.digdag.core;

import java.util.List;
import com.google.common.base.Optional;

public interface TaskControlStore
{
    long addSubtask(Task task);

    void addDependencies(long downstream, List<Long> upstreams);

    // state of all children is one of TaskControlStore.doneStates
    boolean isAllChildrenDone(long taskId);

    // getChildErrors including this task's error
    List<ConfigSource> collectChildrenErrors(long taskId);

    boolean setState(long taskId, TaskStateCode beforeState, TaskStateCode afterState);

    // planned to success
    boolean setStateWithSuccessDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, ConfigSource stateParams, ConfigSource carryParams, TaskReport report);

    // planned to error
    boolean setStateWithErrorDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, ConfigSource stateParams, Optional<Integer> retryInterval, ConfigSource error);

    boolean setStateWithStateParamsUpdate(long taskId, TaskStateCode beforeState, TaskStateCode afterState, ConfigSource stateParams, Optional<Integer> retryInterval);

    int trySetChildrenBlockedToReadyOrShortCircuitPlanned(long taskId);
}
