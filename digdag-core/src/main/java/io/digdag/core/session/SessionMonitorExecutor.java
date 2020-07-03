package io.digdag.core.session;

import java.time.Instant;
import javax.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.Limits;
import io.digdag.core.database.TransactionManager;
import io.digdag.spi.metrics.DigdagMetrics;
import static io.digdag.spi.metrics.DigdagMetrics.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.BackgroundExecutor;
import io.digdag.core.ErrorReporter;
import io.digdag.core.workflow.TaskControl;
import io.digdag.core.workflow.Tasks;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.client.config.ConfigFactory;

public class SessionMonitorExecutor
        implements BackgroundExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(SessionMonitorExecutor.class);

    private final ConfigFactory cf;
    private final SessionStoreManager sm;
    private final WorkflowExecutor exec;
    private final TransactionManager tm;
    private final Limits limits;
    private ScheduledExecutorService executor;

    @Inject(optional = true)
    private ErrorReporter errorReporter = ErrorReporter.empty();

    @Inject
    private DigdagMetrics metrics;

    @Inject
    public SessionMonitorExecutor(
            ConfigFactory cf,
            SessionStoreManager sm,
            TransactionManager tm,
            WorkflowExecutor exec,
            Limits limits)
    {
        this.cf = cf;
        this.sm = sm;
        this.tm = tm;
        this.exec = exec;
        this.limits = limits;
    }

    @PostConstruct
    public synchronized void start()
    {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("session-monitor-scheduler-%d")
                    .build()
                    );
        }
        // TODO make interval configurable?
        executor.scheduleWithFixedDelay(() -> run(),
                1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public synchronized void shutdown()
    {
        if (executor != null) {
            executor.shutdown();
            // TODO wait for shutdown completion?
            executor = null;
        }
    }

    @Override
    public void eagerShutdown()
    {
        shutdown();
    }

    public void run()
    {
        try {
            tm.begin(() -> {
                sm.lockReadySessionMonitors(Instant.now(), (storedMonitor) -> {
                    // runMonitor needs to return next runtime if this monitor should run again later
                    return runMonitor(storedMonitor);
                });
                return null;
            });
        }
        catch (Throwable t) {
            logger.error("An uncaught exception is ignored. This session monitor scheduling will be retried.", t);
            errorReporter.reportUncaughtError(t);
            metrics.increment(Category.DEFAULT, "uncaughtErrors");
        }
    }

    public Optional<Instant> runMonitor(StoredSessionMonitor storedMonitor)
    {
        sm.lockAttemptIfExists(storedMonitor.getAttemptId(), (sessionAttemptControlStore, summary) -> {
            if (!summary.getStateFlags().isDone()) {
                try {
                    return sessionAttemptControlStore.lockRootTask(summary.getId(), (store, storedTask) -> {
                        if (!Tasks.isDone(storedTask.getState())) {
                            exec.addMonitorTask(new TaskControl(store, storedTask, limits), storedMonitor.getType(), storedMonitor.getConfig());
                            return true;
                        }
                        else {
                            return false;
                        }
                    });
                }
                catch (ResourceNotFoundException ex) {
                    // succeeded to lock session attempt but root task doesn't exist.
                    // this must not happen but ok to ignore
                    return false;
                }
            }
            else {
                return false;
            }
        }).or(false);

        // for now, there're no session monitor types that run monitor twice or more
        return Optional.absent();
    }
}
