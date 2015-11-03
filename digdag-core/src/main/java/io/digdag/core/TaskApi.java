package io.digdag.core;

import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.config.Config;

public interface TaskApi
{
    void taskSucceeded(long taskId,
            Config stateParams, Config subtaskConfig,
            TaskReport report);

    void taskFailed(long taskId,
            Config error, Config stateParams,
            Optional<Integer> retryInterval);

    void taskPollNext(long taskId,
            Config stateParams, int retryInterval);
}
