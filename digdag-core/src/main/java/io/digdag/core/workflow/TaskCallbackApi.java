package io.digdag.core.workflow;

import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.spi.TaskReport;
import io.digdag.spi.config.Config;

public interface TaskCallbackApi
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
