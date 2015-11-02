package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.Locale;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionMonitorExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(SessionMonitorExecutor.class);

    private final ConfigSourceFactory cf;
    private final SessionStoreManager sm;
    private final SessionExecutor exec;
    private final ExecutorService executor;

    @Inject
    public SessionMonitorExecutor(
            ConfigSourceFactory cf,
            SessionStoreManager sm,
            SessionExecutor exec)
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
        sm.lockRootTask(storedMonitor.getSessionId(), (TaskControl control, StoredTask detail) -> {
            if (!Tasks.isDone(detail.getState())) {
                exec.addSlaTask(control, detail, storedMonitor.getConfig());
            }
            return true;
        });
        return Optional.absent();
    }
}
