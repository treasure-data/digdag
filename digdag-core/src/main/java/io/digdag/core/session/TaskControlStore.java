package io.digdag.core.session;

import java.util.List;
import java.util.Map;
import com.google.common.base.Optional;
import io.digdag.spi.TaskResult;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;

public interface TaskControlStore
{
    long getTaskCountOfAttempt(long attemptId);

    long addSubtask(long attemptId, Task task);

    long addResumedSubtask(long attemptId, long parentId,
            TaskType taskType, TaskStateCode state, TaskStateFlags flags,
            ResumingTask resumingTask);

    void addResumingTasks(long attemptId, List<ResumingTask> fullNameToTasks);

    List<ResumingTask> getResumingTasksByNamePrefix(long attemptId, String fullNamePrefix);

    //To keep signature compatibility.
    default boolean copyInitialTasksForRetry(List<Long> recursiveChildrenIdList)
    {
        return copyInitialTasksForRetry(recursiveChildrenIdList, Optional.absent());
    }

    boolean copyInitialTasksForRetry(List<Long> recursiveChildrenIdList, Optional<String> parentFullName);

    void addDependencies(long downstream, List<Long> upstreams);

    // return true if one or more child task is progressible.
    boolean isAnyProgressibleChild(long taskId);

    // return true if one or more child task is ERROR or GROUP_ERROR state.
    boolean isAnyErrorChild(long taskId);

    int trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled(long taskId);

    // getChildErrors including this task's error
    List<Config> collectChildrenErrors(long taskId);

    boolean setState(long taskId, TaskStateCode beforeState, TaskStateCode afterState);

    boolean setStartedState(long taskId, TaskStateCode beforeState, TaskStateCode afterState);

    boolean setDoneState(long taskId, TaskStateCode beforeState, TaskStateCode afterState);

    // running to success
    boolean setSuccessStateShortCircuit(long taskId, TaskStateCode beforeState, TaskStateCode afterState, TaskResult result);

    // running to error
    boolean setErrorStateShortCircuit(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config error);

    // planned to success
    boolean setPlannedStateSuccessful(long taskId, TaskStateCode beforeState, TaskStateCode afterState, TaskResult result);

    boolean setPlannedStateWithDelayedError(long taskId, TaskStateCode beforeState, TaskStateCode afterState, int newFlags, Optional<Config> updateError);

    boolean setRetryWaitingState(long taskId, TaskStateCode beforeState, TaskStateCode afterState, int retryInterval, Config stateParams, Optional<Config> updateError);

    //// trySetChildrenBlockedToReadyOrShortCircuitPlanned for root task
    //boolean trySetBlockedToReadyOrShortCircuitPlanned(long taskId);
}
