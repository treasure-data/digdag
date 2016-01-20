package io.digdag.core.session;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.spi.TaskReport;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;

public interface TaskControlStore
{
    long addSubtask(Task task);

    StoredTask getTaskById(long taskId)
        throws ResourceNotFoundException;

    void addDependencies(long downstream, List<Long> upstreams);

    // return true if one or more child task is progressible.
    boolean isAnyProgressibleChild(long taskId);

    // getChildErrors including this task's error
    List<Config> collectChildrenErrors(long taskId);

    boolean setState(long taskId, TaskStateCode beforeState, TaskStateCode afterState);

    // planned to success
    boolean setStateWithSuccessDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, TaskReport report);

    // planned to error
    boolean setStateWithErrorDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, Optional<Integer> retryInterval, Config error);

    boolean setStateWithStateParamsUpdate(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, Optional<Integer> retryInterval);

    int trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled(long taskId);

    //// trySetChildrenBlockedToReadyOrShortCircuitPlanned for root task
    //boolean trySetBlockedToReadyOrShortCircuitPlanned(long taskId);
}
