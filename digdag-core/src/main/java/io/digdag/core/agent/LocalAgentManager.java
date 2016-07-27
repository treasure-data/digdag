package io.digdag.core.agent;

import java.util.function.Supplier;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.queue.TaskQueueServerManager;

public class LocalAgentManager
{
    private final Supplier<MultiThreadAgent> agentFactory;
    private Thread thread;
    private MultiThreadAgent agent;

    @Inject
    public LocalAgentManager(
            AgentConfig config,
            AgentId agentId,
            TaskServerApi taskServer,
            OperatorManager operatorManager)
    {
        if (config.getEnabled()) {
            this.agentFactory = () -> new MultiThreadAgent(config, agentId, taskServer, operatorManager);
        }
        else {
            this.agentFactory = null;
        }
    }

    @PostConstruct
    public void start()
    {
        if (agentFactory != null) {
            agent = agentFactory.get();
            Thread thread = new ThreadFactoryBuilder()
                .setDaemon(false)  // tasks taken from the queue should be certainly processed or callbacked to the server
                .setNameFormat("local-agent-%d")
                .build()
                .newThread(agent);
            thread.start();
            this.thread = thread;
        }
    }

    @PreDestroy
    public void shutdown()
        throws InterruptedException
    {
        if (thread != null) {
            agent.shutdown();
            thread.join();
        }
    }
}
