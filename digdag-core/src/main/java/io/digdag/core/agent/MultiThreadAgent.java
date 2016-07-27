package io.digdag.core.agent;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import com.google.common.base.Optional;
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
    private final ThreadPoolExecutor executor;
    private final Object shutdownLock = new Object();
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
            this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(config.getMaxThreads(), threadFactory);
        }
        else {
            this.executor = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);
        }
    }

    public void shutdown(Optional<Duration> maximumCompletionWait)
        throws InterruptedException
    {
        stop = true;
        taskServer.interruptLocalWait();
        synchronized (shutdownLock) {
            // synchronize shutdownLock not to reject task execution after acquiring them from taskServer
            executor.shutdown();
            shutdownLock.notifyAll();
        }
        if (maximumCompletionWait.isPresent()) {
            long seconds = maximumCompletionWait.get().getSeconds();
            if (!executor.awaitTermination(seconds, TimeUnit.SECONDS)) {
                logger.warn("Some tasks didn't finish within maximum wait time ({} seconds)", seconds);
            }
        }
        else {
            // no maximum wait time. waits for ever
            while (!executor.awaitTermination(24, TimeUnit.HOURS))
                ;
        }
    }

    @Override
    public void run()
    {
        while (!stop) {
            try {
                synchronized (shutdownLock) {
                    if (executor.isShutdown()) {
                        break;
                    }
                    int max = Math.min(executor.getMaximumPoolSize() - executor.getActiveCount(), 10);
                    if (max > 0) {
                        List<TaskRequest> reqs = taskServer.lockSharedAgentTasks(max, agentId, config.getLockRetentionTime(), 1000);
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
                    else {
                        // no executor thread is available. sleep for a while until a task execution finishes
                        shutdownLock.wait(500);
                    }
                }
            }
            catch (Throwable t) {
                logger.error("Uncaught exception. Ignoring.", t);
            }
        }
    }
}
