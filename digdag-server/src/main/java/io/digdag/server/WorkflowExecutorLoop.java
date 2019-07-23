package io.digdag.server;

import com.google.inject.Inject;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.digdag.core.BackgroundExecutor;
import io.digdag.core.ErrorReporter;
import io.digdag.core.workflow.WorkflowExecutor;

import io.digdag.spi.metrics.DigdagMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

public class WorkflowExecutorLoop
        implements BackgroundExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutorLoop.class);

    private final Supplier<Thread> threadFactory;
    private final WorkflowExecutor workflowExecutor;

    private volatile Thread thread = null;
    private volatile boolean stop = false;

    @Inject(optional = true)
    private ErrorReporter errorReporter = ErrorReporter.empty();

    @Inject
    private DigdagMetrics metrics;

    @Inject
    public WorkflowExecutorLoop(
            ServerConfig serverConfig,
            WorkflowExecutor workflowExecutor)
    {
        if (serverConfig.getExecutorEnabled()) {
            this.threadFactory = () -> new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("workflow-executor-%d")
                .build()
                .newThread(this::run);
        }
        else {
            this.threadFactory = null;
        }
        this.workflowExecutor = workflowExecutor;
    }

    private void run()
    {
        while (!stop) {
            try {
                workflowExecutor.runWhile(() -> !stop);
            }
            catch (Throwable t) {
                logger.error("Uncaught error during executing workflow state machine. Ignoring. Loop will be retried.", t);
                errorReporter.reportUncaughtError(t);
                metrics.increment("executor", "uncaughtErrors");
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

    @PostConstruct
    public synchronized void start()
    {
        if (threadFactory != null && thread == null) {
            Thread thread = threadFactory.get();
            thread.start();
            this.thread = thread;
        }
    }

    @PreDestroy
    public synchronized void shutdown()
        throws InterruptedException
    {
        startShutdown();

        if (thread != null) {
            thread.join(100);
            if (thread.isAlive()) {
                logger.info("Waiting for completion of workflow executor loop...");
                do {
                    workflowExecutor.noticeRunWhileConditionChange();
                    thread.join(1000);
                } while (thread.isAlive());
            }
            thread = null;
        }
    }

    @Override
    public void eagerShutdown()
            throws InterruptedException
    {
        startShutdown();
    }

    private void startShutdown()
    {
        if (!stop) {
            stop = true;
            logger.info("Shutting down workflow executor loop");

            // noticeRunWhileConditionChange is not 100% accurate because
            // WorkflowExecutor doesn't lock stop flag before check. But
            // it will be repeated in shutdown() method later
            workflowExecutor.noticeRunWhileConditionChange();
        }
    }
}
