package io.digdag.core.agent;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.spi.TaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalAgent
        implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(LocalAgent.class);

    private final AgentConfig config;
    private final AgentId agentId;
    private final TaskServerApi taskServer;
    private final OperatorManager runner;
    private final ExecutorService executor;
    private volatile boolean stop = false;

    public LocalAgent(AgentConfig config, AgentId agentId,
            TaskServerApi taskServer, OperatorManager runner)
    {
        this.agentId = agentId;
        this.config = config;
        this.taskServer = taskServer;
        this.runner = runner;
        if (config.getMaxThreads() > 0) {
            this.executor = Executors.newFixedThreadPool(
                    config.getMaxThreads(),
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("task-thread-%d")
                    .build());
        }
        else {
            this.executor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("task-thread-%d")
                    .build());
        }
    }

    public void stop()
    {
        stop = true;
    }

    public void shutdown()
    {
        executor.shutdown();
        // TODO wait for shutdown completion?
    }

    @Override
    public void run()
    {
        while (!stop) {
            try {
                List<TaskRequest> reqs = taskServer.lockSharedAgentTasks(1, agentId, config.getLockRetentionTime(), 1000);
                for (TaskRequest req : reqs) {
                    executor.submit(() -> {
                        try {
                            runner.run(req);
                        }
                        catch (Throwable t) {
                            logger.error("Uncaught exception. Task heartbeat for at-least-once task execution is not implemented yet.", t);
                        }
                    });
                }
            }
            catch (Throwable t) {
                logger.error("Uncaught exception", t);
            }
        }
    }
}
