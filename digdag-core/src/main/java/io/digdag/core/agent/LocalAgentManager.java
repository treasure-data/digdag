package io.digdag.core.agent;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import io.digdag.spi.TaskQueueClient;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.queue.TaskQueueServerManager;

public class LocalAgentManager
{
    private final AgentConfig config;
    private final AgentId agentId;
    private final TaskQueueClient queueClient;
    private final OperatorManager operatorManager;
    private final ExecutorService executor;

    @Inject
    public LocalAgentManager(
            AgentConfig config,
            AgentId agentId,
            TaskQueueServerManager queueManager,
            OperatorManager operatorManager)
    {
        this.config = config;
        this.agentId = agentId;
        this.queueClient = queueManager.getInProcessTaskQueueClient();
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
                        queueClient,
                        operatorManager
                    )
                );
        }
    }
}
