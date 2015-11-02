package io.digdag.core;

import com.google.common.base.*;
import com.google.common.collect.*;

public interface TaskApi
{
    void taskSucceeded(long taskId,
            ConfigSource stateParams, ConfigSource subtaskConfig,
            TaskReport report);

    void taskFailed(long taskId,
            ConfigSource error, ConfigSource stateParams,
            Optional<Integer> retryInterval);

    void taskPollNext(long taskId,
            ConfigSource stateParams, int retryInterval);
}
