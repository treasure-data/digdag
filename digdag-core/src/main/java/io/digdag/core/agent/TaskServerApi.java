package io.digdag.core.agent;

import java.util.List;

import io.digdag.spi.AccountRouting;
import io.digdag.spi.TaskRequest;

public interface TaskServerApi
{
    List<TaskRequest> lockSharedAgentTasks(
            int count, AgentId agentId,
            int lockSeconds, long maxSleepMillis, AccountRouting accountRouting);

    void interruptLocalWait();
}
