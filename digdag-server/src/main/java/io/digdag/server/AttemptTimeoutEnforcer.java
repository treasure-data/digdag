package io.digdag.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.util.DurationParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class AttemptTimeoutEnforcer
{
    private static final Logger logger = LoggerFactory.getLogger(AttemptTimeoutEnforcer.class);

    private static final Duration DEFAULT_ATTEMP_TTL = Duration.ofDays(1);
    private static final Duration DEFAULT_REAPING_INTERVAL = Duration.ofSeconds(5);

    private final ScheduledExecutorService scheduledExecutorService;
    private final SessionStoreManager ssm;

    private final Duration attemptTTL;
    private final Duration reapingInterval;

    @Inject
    public AttemptTimeoutEnforcer(ServerConfig serverConfig, SessionStoreManager ssm, Config systemConfig)
    {
        this.attemptTTL = systemConfig.getOptional("executor.attempt_ttl", DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(DEFAULT_ATTEMP_TTL);

        this.reapingInterval = systemConfig.getOptional("executor.attempt_reaping_interval", DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(DEFAULT_REAPING_INTERVAL);

        this.ssm = ssm;

        if (serverConfig.getExecutorEnabled()) {
            this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("attempt-timeout-enforcer-%d")
                    .build());
        }
        else {
            this.scheduledExecutorService = null;
        }
    }

    private void run()
    {
        try {
            enforceAttemptTTLs();
        }
        catch (Throwable t) {
            logger.error("Uncaught exception", t);
        }
    }

    private void enforceAttemptTTLs()
    {
        Instant creationDeadline = ssm.getStoreTime().minus(attemptTTL);
        int state = 0;
        long lastId = 0;
        List<StoredSessionAttempt> attempts = ssm.findAttemptsCreatedBeforeWithState(creationDeadline, state, lastId, 100);
        for (StoredSessionAttempt attempt : attempts) {
            logger.info("Session Attempt timed out, canceling: {}", attempt);
            ssm.requestCancelAttempt(attempt.getId());
        }
    }

    @PostConstruct
    public void start()
    {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.scheduleAtFixedRate(this::run, reapingInterval.toNanos(), reapingInterval.toNanos(), NANOSECONDS);
        }
    }

    @PreDestroy
    public void shutdown()
            throws InterruptedException
    {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }
}
