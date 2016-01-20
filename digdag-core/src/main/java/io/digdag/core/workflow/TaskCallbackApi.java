package io.digdag.core.workflow;

import com.google.common.base.Optional;
import io.digdag.spi.TaskReport;
import io.digdag.client.config.Config;
import io.digdag.spi.RevisionInfo;

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

    //boolean requireSession(RevisionInfo revision, String workflowName);
}
