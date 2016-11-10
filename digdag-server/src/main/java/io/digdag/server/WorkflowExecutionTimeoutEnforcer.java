package io.digdag.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.TaskAttemptSummary;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskType;
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

public class WorkflowExecutionTimeoutEnforcer
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutionTimeoutEnforcer.class);

    private static final Duration DEFAULT_ATTEMPT_TTL = Duration.ofDays(7);
    private static final Duration DEFAULT_TASK_TTL = Duration.ofDays(1);
    private static final Duration DEFAULT_REAPING_INTERVAL = Duration.ofSeconds(5);

    private final ScheduledExecutorService scheduledExecutorService;
    private final SessionStoreManager ssm;

    private final Duration attemptTTL;
    private final Duration reapingInterval;
    private final Duration taskTTL;

    @Inject
    public WorkflowExecutionTimeoutEnforcer(ServerConfig serverConfig, SessionStoreManager ssm, Config systemConfig)
    {
        this.attemptTTL = systemConfig.getOptional("executor.attempt_ttl", DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(DEFAULT_ATTEMPT_TTL);

        this.taskTTL = systemConfig.getOptional("executor.task_ttl", DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(DEFAULT_TASK_TTL);

        this.reapingInterval = systemConfig.getOptional("executor.ttl_reaping_interval", DurationParam.class)
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

        try {
            enforceTaskTTLs();
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

        List<StoredSessionAttempt> expiredAttempts = ssm.findAttemptsCreatedBeforeWithState(creationDeadline, state, lastId, 100);

        for (StoredSessionAttempt attempt : expiredAttempts) {
            AttemptStateFlags stateFlags;
            try {
                stateFlags = ssm.getAttemptStateFlags(attempt.getId());
            }
            catch (ResourceNotFoundException e) {
                logger.debug("Session Attempt not found, ignoring: {}", attempt, e);
                continue;
            }

            if (stateFlags.isCancelRequested()) {
                logger.debug("Session Attempt already canceled, ignoring: {}", attempt);
                continue;
            }

            logger.info("Session Attempt timed out, canceling: {}", attempt);
            ssm.requestCancelAttempt(attempt.getId());
        }
    }

    private void enforceTaskTTLs()
    {
        Instant startDeadline = ssm.getStoreTime().minus(taskTTL);
        long lastId = 0;

        List<TaskAttemptSummary> expiredTasks = ssm.findTasksStartedBeforeWithStateAndType(TaskType.of(0), TaskStateCode.notDoneStates(), startDeadline, lastId, 100);

        for (TaskAttemptSummary task : expiredTasks) {
            AttemptStateFlags stateFlags;
            try {
                stateFlags = ssm.getAttemptStateFlags(task.getAttemptId());
            }
            catch (ResourceNotFoundException e) {
                logger.debug("Session Attempt not found, ignoring: {}", task, e);
                continue;
            }

            if (stateFlags.isCancelRequested()) {
                logger.debug("Session Attempt already canceled, ignoring: {}", task);
                continue;
            }

            logger.info("Task timed out, canceling Session Attempt: {}", task);
            ssm.requestCancelAttempt(task.getAttemptId());
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
