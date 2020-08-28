package io.digdag.server;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.log.LogMarkers;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.TaskAttemptSummary;
import io.digdag.core.session.TaskStateCode;
import io.digdag.spi.Notification;
import io.digdag.spi.NotificationException;
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

    // This is similar to TaskStateCode.notDoneStates() but BLOCKED, PLANNED, and READY are excluded.
    // BLOCKED and PLANNED are excluded because there're other running tasks that should be enforced instead.
    // READY is excluded the workflow itself is working correctly and number of threads is insufficient.
    private static final TaskStateCode[] TASK_TTL_ENFORCED_STATE_CODES = new TaskStateCode[] {
        TaskStateCode.RETRY_WAITING,
        TaskStateCode.GROUP_RETRY_WAITING,
        TaskStateCode.RUNNING,
    };

    private final ScheduledExecutorService scheduledExecutorService;
    private final SessionStoreManager ssm;
    private final Notifier notifier;
    private final ProjectStoreManager psm;
    private final TransactionManager tm;

    private final Duration attemptTTL;
    private final Duration reapingInterval;
    private final Duration taskTTL;

    @Inject
    public WorkflowExecutionTimeoutEnforcer(
            ServerConfig serverConfig,
            SessionStoreManager ssm,
            TransactionManager tm,
            Config systemConfig,
            Notifier notifier,
            ProjectStoreManager psm)
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
        this.notifier = notifier;
        this.psm = psm;
        this.tm = tm;

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
            logger.error(
                    LogMarkers.UNEXPECTED_SERVER_ERROR,
                    "Uncaught exception when enforcing attempt TTLs. Ignoring. Loop will be retried.", t);
        }

        try {
            enforceTaskTTLs();
        }
        catch (Throwable t) {
            logger.error(
                    LogMarkers.UNEXPECTED_SERVER_ERROR,
                    "Uncaught exception when enforcing task TTLs. Ignoring. Loop will be retried.", t);
        }
    }

    private void enforceAttemptTTLs()
    {
        List<StoredSessionAttempt> expiredAttempts = tm.begin(() -> {
            Instant creationDeadline = ssm.getStoreTime().minus(attemptTTL);
            return ssm.findActiveAttemptsCreatedBefore(creationDeadline, (long) 0, 100);
        });

        for (StoredSessionAttempt attempt : expiredAttempts) {
            try {
                boolean canceled = tm.begin(() -> {
                    AttemptStateFlags stateFlags;
                    try {
                        stateFlags = ssm.getAttemptStateFlags(attempt.getId());
                    }
                    catch (ResourceNotFoundException e) {
                        logger.debug("Session Attempt not found, ignoring: {}", attempt, e);
                        return null;
                    }

                    if (stateFlags.isCancelRequested()) {
                        logger.debug("Session Attempt already canceled, ignoring: {}", attempt);
                        return null;
                    }

                    logger.info("Session Attempt timed out, canceling: {}", attempt);
                    return ssm.requestCancelAttempt(attempt.getId());
                });

                if (canceled) {
                    sendTimeoutNotification("Workflow execution timeout", attempt.getId());
                }
            }
            catch (Throwable t) {
                logger.error(
                        LogMarkers.UNEXPECTED_SERVER_ERROR,
                        "Uncaught exception when enforcing attempt TTLs of attempt {}. Ignoring. Loop continues.", attempt.getId(), t);
            }
        }
    }

    private void enforceTaskTTLs()
    {
        List<TaskAttemptSummary> expiredTasks = tm.begin(() -> {
            Instant startDeadline = ssm.getStoreTime().minus(taskTTL);
            return ssm.findTasksStartedBeforeWithState(TASK_TTL_ENFORCED_STATE_CODES, startDeadline, (long) 0, 100);
        });

        Map<Long, List<TaskAttemptSummary>> attempts = expiredTasks.stream()
                .collect(groupingBy(TaskAttemptSummary::getAttemptId));

        for (Map.Entry<Long, List<TaskAttemptSummary>> entry : attempts.entrySet()) {
            long attemptId = entry.getKey();
            try {
                boolean canceled = tm.begin(() -> {
                    logger.info("Task(s) timed out, canceling Session Attempt: {}, tasks={}", attemptId, entry.getValue());
                    return ssm.requestCancelAttempt(attemptId);
                });

                if (canceled) {
                    String taskIds = entry.getValue().stream().mapToLong(TaskAttemptSummary::getId).mapToObj(Long::toString).collect(joining(","));
                    sendTimeoutNotification("Task execution timeout: " + taskIds, attemptId);
                }
            }
            catch (Throwable t) {
                logger.error(
                        LogMarkers.UNEXPECTED_SERVER_ERROR,
                        "Uncaught exception when enforcing task TTLs of attempt {}. Ignoring. Loop continues.", entry.getKey(), t);
            }
        }
    }

    private void sendTimeoutNotification(String message, long attemptId)
    {
        StoredSessionAttemptWithSession attempt;
        try {
            attempt = tm.begin(() -> ssm.getAttemptWithSessionById(attemptId), ResourceNotFoundException.class);
        }
        catch (ResourceNotFoundException e) {
            logger.error("Session attempt not found, ignoring: {}", attemptId);
            return;
        }

        int projectId = attempt.getSession().getProjectId();
        StoredProject project;
        try {
            project = tm.begin(() -> psm.getProjectByIdInternal(projectId), ResourceNotFoundException.class);
        }
        catch (ResourceNotFoundException e) {
            logger.error("Project not found, ignoring: {}", attemptId);
            return;
        }

        Optional<Long> wfId = attempt.getWorkflowDefinitionId();

        Optional<StoredWorkflowDefinitionWithProject> workflow = Optional.absent();
        if (wfId.isPresent()) {
            try {
                workflow = Optional.of(tm.begin(() -> psm.getWorkflowDetailsById(wfId.get()), ResourceNotFoundException.class));
            }
            catch (ResourceNotFoundException e) {
                workflow = Optional.absent();
            }
        }

        Notification notification = Notification.builder(Instant.now(), message)
                .attemptId(attempt.getId())
                .projectId(projectId)
                .projectName(project.getName())
                .revision(workflow.transform(wf -> wf.getRevisionName()))
                .sessionId(attempt.getSessionId())
                .siteId(attempt.getSiteId())
                .workflowName(workflow.transform(wf -> wf.getName()))
                .workflowDefinitionId(wfId)
                .build();

        try {
            // Assuming this method creates a new database transaction if needed. So this method call
            // is in outside of tm.begin block.
            notifier.sendNotification(notification);
        }
        catch (NotificationException e) {
            logger.error("Failed to send execution timeout notification for attempt: {}", attemptId, e);
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
