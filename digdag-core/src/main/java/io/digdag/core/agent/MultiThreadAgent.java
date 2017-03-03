package io.digdag.core.agent;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.BlockingQueue;
import java.time.Duration;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.spi.TaskRequest;
import io.digdag.core.ErrorReporter;
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
    private final ErrorReporter errorReporter;

    private final Object newTaskLock = new Object();
    private final BlockingQueue<Runnable> executorQueue;
    private final ThreadPoolExecutor executor;

    private volatile boolean stop = false;

    public MultiThreadAgent(
            AgentConfig config, AgentId agentId,
            TaskServerApi taskServer, OperatorManager runner,
            ErrorReporter errorReporter)
    {
        this.agentId = agentId;
        this.config = config;
        this.taskServer = taskServer;
        this.runner = runner;
        this.errorReporter = errorReporter;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(false)  // make them non-daemon threads so that shutting down agent doesn't kill operator execution
            .setNameFormat("task-thread-%d")
            .build();

        if (config.getMaxThreads() > 0) {
            this.executorQueue = new LinkedBlockingQueue<Runnable>();
            this.executor = new ThreadPoolExecutor(
                    0, config.getMaxThreads(),
                    60L, TimeUnit.SECONDS,
                    executorQueue, threadFactory);
        }
        else {
            // If there're no upper limit on number of threads, queue actually doesn't need to store entries.
            // Instead, executor.submit() blocks until a thread starts and takes it.
            // SynchronousQueue.size() always returns 0.
            this.executorQueue = new SynchronousQueue<Runnable>();
            this.executor = new ThreadPoolExecutor(
                    0, Integer.MAX_VALUE,
                    60L, TimeUnit.SECONDS,
                    executorQueue, threadFactory);
        }
    }

    public void shutdown(Optional<Duration> maximumCompletionWait)
        throws InterruptedException
    {
        stop = true;
        taskServer.interruptLocalWait();
        int maximumPossibleActiveTaskCount;
        synchronized (newTaskLock) {
            // synchronize newTaskLock not to reject task execution after acquiring them from taskServer
            executor.shutdown();
            maximumPossibleActiveTaskCount = executorQueue.size() + executor.getActiveCount();
            newTaskLock.notifyAll();
        }
        if (maximumPossibleActiveTaskCount > 0) {
            logger.info("Waiting for completion of {} running tasks...", maximumPossibleActiveTaskCount);
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
                synchronized (newTaskLock) {
                    if (executor.isShutdown()) {
                        break;
                    }
                    int maximumPossibleActiveTaskCount = executorQueue.size() + executor.getActiveCount();
                    int guaranteedAvaialbleThreads = executor.getMaximumPoolSize() - maximumPossibleActiveTaskCount;
                    int maxAcquire = Math.min(guaranteedAvaialbleThreads, 10);
                    if (maxAcquire > 0) {
                        List<TaskRequest> reqs = taskServer.lockSharedAgentTasks(maxAcquire, agentId, config.getLockRetentionTime(), 1000);
                        for (TaskRequest req : reqs) {
                            executor.submit(() -> {
                                try {
                                    runner.run(req);
                                }
                                catch (Throwable t) {
                                    logger.error("Uncaught exception. Task queue will detect this failure and this task will be retried later.", t);
                                    errorReporter.reportUncaughtError(t);
                                }
                            });
                        }
                    }
                    else {
                        // no executor thread is available. sleep for a while until a task execution finishes
                        newTaskLock.wait(500);
                    }
                }
            }
            catch (Throwable t) {
                logger.error("Uncaught exception during acquiring tasks from a server. Ignoring. Agent thread will be retried.", t);
                errorReporter.reportUncaughtError(t);
                try {
                    // sleep before retrying
                    Thread.sleep(1000);
                }
                catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
