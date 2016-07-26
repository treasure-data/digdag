package io.digdag.core.agent;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.spi.TaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiThreadAgent
        implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MultiThreadAgent.class);

    private final AgentConfig config;
    private final AgentId agentId;
    private final TaskServerApi taskServer;
    private final OperatorManager runner;
    private final ExecutorService executor;
    private volatile boolean stop = false;

    public MultiThreadAgent(AgentConfig config, AgentId agentId,
            TaskServerApi taskServer, OperatorManager runner)
    {
        this.agentId = agentId;
        this.config = config;
        this.taskServer = taskServer;
        this.runner = runner;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(false)  // make them non-daemon threads so that shutting down agent doesn't kill operator execution
            .setNameFormat("task-thread-%d")
            .build();
        if (config.getMaxThreads() > 0) {
            this.executor = Executors.newFixedThreadPool(config.getMaxThreads(), threadFactory);
        }
        else {
            this.executor = Executors.newCachedThreadPool(threadFactory);
        }
    }

    public void shutdown()
    {
        stop = true;
        taskServer.interruptLocalWait();
        executor.shutdown();
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
