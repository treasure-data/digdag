package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.ErrorReporter;
import io.digdag.core.database.TransactionManager;
import io.digdag.metrics.DigdagMetrics;
import io.digdag.spi.TaskRequest;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
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
    private final TransactionManager transactionManager;
    private final ErrorReporter errorReporter;

    private final Object addActiveTaskLock = new Object();
    private final BlockingQueue<Runnable> executorQueue;
    private final ThreadPoolExecutor executor;
    private final AtomicInteger activeTaskCount = new AtomicInteger(0);
    private final DigdagMetrics metrics;

    private volatile boolean stop = false;

    public MultiThreadAgent(
            AgentConfig config, AgentId agentId,
            TaskServerApi taskServer, OperatorManager runner,
            TransactionManager transactionManager, ErrorReporter errorReporter,DigdagMetrics metrics)
    {
        this.agentId = agentId;
        this.config = config;
        this.taskServer = taskServer;
        this.runner = runner;
        this.transactionManager = transactionManager;
        this.errorReporter = errorReporter;
        this.metrics = metrics;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(false)  // make them non-daemon threads so that shutting down agent doesn't kill operator execution
            .setNameFormat("task-thread-%d")
            .build();

        if (config.getMaxThreads() > 0) {
            this.executorQueue = new LinkedBlockingQueue<Runnable>();
            this.executor = new ThreadPoolExecutor(
                    config.getMaxThreads(), config.getMaxThreads(),
                    0L, TimeUnit.SECONDS,
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
        int maximumActiveTasks;
        synchronized (addActiveTaskLock) {
            // synchronize addActiveTaskLock not to reject task execution after acquiring them from taskServer
            executor.shutdown();  // Since here, no one can increase activeTaskCount.
            maximumActiveTasks = activeTaskCount.get();  /// Now get the maximum count.
            addActiveTaskLock.notifyAll();
        }
        if (maximumActiveTasks > 0) {
            logger.info("Waiting for completion of {} running tasks...", maximumActiveTasks);
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
                synchronized (addActiveTaskLock) {
                    if (executor.isShutdown()) {
                        break;
                    }
                    // Because addActiveTaskLock is locked, no one increases activeTaskCount in this synchronized block. Now get the maximum count.
                    int maximumActiveTasks = activeTaskCount.get();
                    // Because the maximum count doesn't increase, here can know that at least N number of threads are idling.
                    int guaranteedAvaialbleThreads = executor.getMaximumPoolSize() - maximumActiveTasks;
                    // Acquire at most guaranteedAvaialbleThreads or 10. This guarantees that all tasks start immediately.
                    int maxAcquire = Math.min(guaranteedAvaialbleThreads, 10);
                    if (maxAcquire > 0) {
                        metrics.gauge("AGENT_NumMaxAcquire", maxAcquire);
                        transactionManager.begin(() -> {
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
                                    finally {
                                        activeTaskCount.decrementAndGet();
                                    }
                                });
                                activeTaskCount.incrementAndGet();
                            }
                            return null;
                        });
                    }
                    else {
                        metrics.increment("AGENT_RunWaitCounter");
                        // no executor thread is available. sleep for a while until a task execution finishes
                        addActiveTaskLock.wait(500);
                    }
                }
            }
            catch (Throwable t) {
                logger.error("Uncaught exception during acquiring tasks from a server. Ignoring. Agent thread will be retried.", t);
                errorReporter.reportUncaughtError(t);
                metrics.increment("AGENT_UncaughtExceptionCounter");
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
