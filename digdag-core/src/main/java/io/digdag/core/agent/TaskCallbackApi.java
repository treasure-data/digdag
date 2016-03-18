package io.digdag.core.agent;

import java.util.List;
import java.time.Instant;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.core.log.TaskLogger;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskRequest;
import io.digdag.core.session.SessionStateFlags;

public interface TaskCallbackApi
{
    TaskLogger newTaskLogger(TaskRequest request);

    void taskHeartbeat(int siteId,
            List<String> lockedIds, AgentId agentId, int lockSeconds);

    void taskSucceeded(int siteId,
            long taskId, String lockId, AgentId agentId,
            TaskResult result);

    void taskFailed(int siteId,
            long taskId, String lockId, AgentId agentId,
            Config error);

    void retryTask(int siteId,
            long taskId, String lockId, AgentId agentId,
            int retryInterval, Config retryStateParams,
            Optional<Config> error);

    SessionStateFlags startSession(
            int siteId,
            int repositoryId,
            String workflowName,
            Instant instant,
            Optional<String> retryAttemptName,
            Config overwriteParams);
}
