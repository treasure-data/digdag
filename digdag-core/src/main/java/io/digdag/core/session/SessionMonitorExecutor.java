package io.digdag.core.session;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.workflow.TaskControl;
import io.digdag.core.workflow.Tasks;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.client.config.ConfigFactory;

public class SessionMonitorExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(SessionMonitorExecutor.class);

    private final ConfigFactory cf;
    private final SessionStoreManager sm;
    private final WorkflowExecutor exec;
    private final ScheduledExecutorService executor;

    @Inject
    public SessionMonitorExecutor(
            ConfigFactory cf,
            SessionStoreManager sm,
            WorkflowExecutor exec)
    {
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("session-monitor-scheduler-%d")
                .build()
                );
        this.cf = cf;
        this.sm = sm;
        this.exec = exec;
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdown();
        // TODO wait for shutdown completion?
    }

    public void start()
    {
        // TODO sleep interval
        executor.scheduleWithFixedDelay(() -> run(),
                1, 1, TimeUnit.SECONDS);
    }

    public void run()
    {
        try {
            sm.lockReadySessionMonitors(Instant.now(), (storedMonitor) -> {
                // runMonitor needs to return next runtime if this monitor should run again later
                return runMonitor(storedMonitor);
            });
        }
        catch (Throwable t) {
            logger.error("An uncaught exception is ignored. This session monitor scheduling will be retried.", t);
        }
    }

    public Optional<Instant> runMonitor(StoredSessionMonitor storedMonitor)
    {
        sm.lockAttemptIfExists(storedMonitor.getAttemptId(), (sessionAttemptControlStore, summary) -> {
            if (!summary.getStateFlags().isDone()) {
                try {
                    return sessionAttemptControlStore.lockRootTask(summary.getId(), (store, storedTask) -> {
                        if (!Tasks.isDone(storedTask.getState())) {
                            exec.addMonitorTask(new TaskControl(store, storedTask), storedMonitor.getType(), storedMonitor.getConfig());
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
