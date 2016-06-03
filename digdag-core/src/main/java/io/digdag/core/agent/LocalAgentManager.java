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
    private final OperatorManager operatorManager;
    private final ExecutorService executor;

    @Inject
    public LocalAgentManager(
            AgentConfig config,
            AgentId agentId,
            TaskQueueManager queueManager,
            OperatorManager operatorManager)
    {
        this.config = config;
        this.agentId = agentId;
        this.queueManager = queueManager;
        this.operatorManager = operatorManager;
        if (config.getEnabled()) {
            this.executor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("local-agent-%d")
                    .build());
        }
        else {
            this.executor = null;
        }
    }

    // TODO stop LocalAgent at @PreDestroy

    public void start()
    {
        if (executor != null) {
            executor.submit(
                    new LocalAgent(
                        config,
                        agentId,
                        queueManager.getInProcessTaskQueueClient(),
                        operatorManager
                    )
                );
        }
    }
}
