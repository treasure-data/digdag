package io.digdag.core.workflow;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.spi.TaskReport;
import io.digdag.core.config.Config;

public interface TaskControlStore
{
    long addSubtask(Task task);

    StoredTask getTaskById(long taskId);

    void addDependencies(long downstream, List<Long> upstreams);

    // state of all children is one of TaskControlStore.doneStates
    boolean isAllChildrenDone(long taskId);

    // getChildErrors including this task's error
    List<Config> collectChildrenErrors(long taskId);

    boolean setState(long taskId, TaskStateCode beforeState, TaskStateCode afterState);

    // planned to success
    boolean setStateWithSuccessDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, TaskReport report);

    // planned to error
    boolean setStateWithErrorDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, Optional<Integer> retryInterval, Config error);

    boolean setStateWithStateParamsUpdate(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, Optional<Integer> retryInterval);

    int trySetChildrenBlockedToReadyOrShortCircuitPlanned(long taskId);

    //// trySetChildrenBlockedToReadyOrShortCircuitPlanned for root task
    //boolean trySetBlockedToReadyOrShortCircuitPlanned(long taskId);
}
