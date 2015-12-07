package io.digdag.core.session;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.workflow.Tasks;
import io.digdag.core.workflow.WorkflowExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.config.ConfigFactory;

public class SessionMonitorExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(SessionMonitorExecutor.class);

    private final ConfigFactory cf;
    private final SessionStoreManager sm;
    private final WorkflowExecutor exec;
    private final ExecutorService executor;

    @Inject
    public SessionMonitorExecutor(
            ConfigFactory cf,
            SessionStoreManager sm,
            WorkflowExecutor exec)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("session-monitor-%d")
                .build()
                );
        this.cf = cf;
        this.sm = sm;
        this.exec = exec;
    }

    public void start()
    {
        executor.submit(() -> run());
    }

    public void run()
    {
        try {
            while (true) {
                Thread.sleep(1000);  // TODO sleep interval
                sm.lockReadySessionMonitors(new Date(), (storedMonitor) -> {
                    return runMonitor(storedMonitor);
                });
            }
        }
        catch (Throwable t) {
            logger.error("Uncaught exception", t);
        }
    }

    public Optional<Date> runMonitor(StoredSessionMonitor storedMonitor)
    {
        sm.lockRootTaskIfExists(storedMonitor.getSessionId(), (TaskControl control, StoredTask detail) -> {
            if (!Tasks.isDone(detail.getState())) {
                exec.addSlaTask(control, detail, storedMonitor.getConfig());
            }
            return true;
        });
        // do nothing if already deleted
        return Optional.absent();
    }
}
