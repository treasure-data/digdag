package io.digdag.core.agent;

import java.util.List;
import io.digdag.spi.TaskRequest;

public interface TaskServerApi
{
    List<TaskRequest> lockSharedAgentTasks(
            int count, AgentId agentId,
            int lockSeconds, long maxSleepMillis);

    void interruptLocalWait();
}
