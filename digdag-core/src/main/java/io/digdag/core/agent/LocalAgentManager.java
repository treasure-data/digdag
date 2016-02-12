package io.digdag.core.agent;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.queue.TaskQueueManager;

public class LocalAgentManager
{
    private final AgentConfig config;
    private final AgentId agentId;
    private final TaskQueueManager queueManager;
    private final TaskRunnerManager taskRunnerManager;
    private final ExecutorService executor;

    @Inject
    public LocalAgentManager(
            AgentConfig config,
            AgentId agentId,
            TaskQueueManager queueManager,
            TaskRunnerManager taskRunnerManager)
    {
        this.config = config;
        this.agentId = agentId;
        this.queueManager = queueManager;
        this.taskRunnerManager = taskRunnerManager;
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("local-agent-%d")
                .build()
                );
    }

    // TODO stop LocalAgent at @PreDestroy

    public void startLocalAgent(int siteId, String queueName)
    {
        executor.submit(
                new LocalAgent(
                    config,
                    agentId,
                    queueManager.getInProcessTaskQueueClient(siteId),
                    taskRunnerManager
                )
            );
    }
}
