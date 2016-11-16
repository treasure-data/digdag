package io.digdag.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.TaskAttemptSummary;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.workflow.TaskControl;
import io.digdag.core.workflow.Tasks;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.spi.Notifier;
import io.digdag.util.DurationParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;

public class WorkflowExecutionTimeoutEnforcer
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowExecutionTimeoutEnforcer.class);

    private static final Duration DEFAULT_ATTEMPT_TTL = Duration.ofDays(7);
    private static final Duration DEFAULT_TASK_TTL = Duration.ofDays(1);
    private static final Duration DEFAULT_REAPING_INTERVAL = Duration.ofSeconds(5);

    private final ScheduledExecutorService scheduledExecutorService;
    private final SessionStoreManager ssm;

    private final WorkflowExecutor workflowExecutor;

    private final Duration attemptTTL;
    private final Duration reapingInterval;
    private final Duration taskTTL;

    @Inject
    public WorkflowExecutionTimeoutEnforcer(ServerConfig serverConfig, SessionStoreManager ssm, Config systemConfig, Notifier notifier, WorkflowExecutor workflowExecutor)
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
        this.workflowExecutor = workflowExecutor;

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
            logger.error("Uncaught exception when enforcing attempt TTLs. Ignoring. Loop will be retried.", t);
        }

        try {
            enforceTaskTTLs();
        }
        catch (Throwable t) {
            logger.error("Uncaught exception when enforcing task TTLs. Ignoring. Loop will be retried.", t);
        }
    }

    private void enforceAttemptTTLs()
    {
        Instant creationDeadline = ssm.getStoreTime().minus(attemptTTL);

        List<StoredSessionAttempt> expiredAttempts = ssm.findActiveAttemptsCreatedBefore(creationDeadline, (long) 0, 100);

        for (StoredSessionAttempt attempt : expiredAttempts) {
            logger.info("Session Attempt timed out, failing: {}", attempt.getId());
            failAttempt("Attempt timeout", attempt.getId());
        }
    }

    private void enforceTaskTTLs()
    {
        Instant startDeadline = ssm.getStoreTime().minus(taskTTL);

        List<TaskAttemptSummary> expiredTasks = ssm.findTasksStartedBeforeWithState(TaskStateCode.notDoneStates(), startDeadline, (long) 0, 100);

        Map<Long, List<TaskAttemptSummary>> attempts = expiredTasks.stream()
                .collect(groupingBy(TaskAttemptSummary::getAttemptId));

        for (Map.Entry<Long, List<TaskAttemptSummary>> entry : attempts.entrySet()) {
            logger.info("Task(s) timed out, failing Session Attempt: {}, tasks={}", entry.getKey(), entry.getValue());
            String taskIds = entry.getValue().stream().mapToLong(TaskAttemptSummary::getId).mapToObj(Long::toString).collect(joining(","));
            failAttempt("Task timeout: " + taskIds, entry.getKey());
        }
    }

    private void failAttempt(String message, long attemptId)
    {
        AttemptStateFlags stateFlags;
        try {
            stateFlags = ssm.getAttemptStateFlags(attemptId);
        }
        catch (ResourceNotFoundException e) {
            logger.debug("Session Attempt not found, ignoring: {}", attemptId, e);
            return;
        }

        if (stateFlags.isCancelRequested()) {
            logger.debug("Session Attempt already canceled, ignoring: {}", attemptId);
            return;
        }

        ssm.lockAttemptIfExists(attemptId, (sessionAttemptControlStore, summary) -> {
            if (!summary.getStateFlags().isDone()) {
                try {
                    return sessionAttemptControlStore.lockRootTask(summary.getId(), (store, storedTask) -> {
                        if (!Tasks.isDone(storedTask.getState())) {
                            workflowExecutor.addFailureTasks(message, new TaskControl(store, storedTask));
                            return true;
                        }
                        else {
                            return false;
                        }
                    });
                }
                catch (ResourceNotFoundException ex) {
                    logger.warn("Root task not found: attemptId={}", attemptId, ex);
                    return false;
                }
            }
            else {
                return false;
            }
        }).or(false);

//         Mark attempt as canceled so it is excluded in future TTL checks
//        ssm.requestCancelAttempt(attemptId);
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
