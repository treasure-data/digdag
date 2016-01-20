package io.digdag.core.agent;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskReport;
import io.digdag.core.session.Session;
import io.digdag.core.session.TaskStateCode;

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

    TaskStateCode startSession(String repositoryName, String workflowName, Session session);
}
