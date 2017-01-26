package io.digdag.server;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.database.Transaction;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.repository.WorkflowDefinition;
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
            Config systemConfig,
            Notifier notifier,
            ProjectStoreManager psm,
            TransactionManager tm)
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
            tm.begin(() -> {
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

                return null;
            });
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    private void enforceAttemptTTLs()
    {
        Instant creationDeadline = ssm.getStoreTime().minus(attemptTTL);

        List<StoredSessionAttempt> expiredAttempts = ssm.findActiveAttemptsCreatedBefore(creationDeadline, (long) 0, 100);

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
            boolean canceled = ssm.requestCancelAttempt(attempt.getId());
            if (canceled) {
                sendTimeoutNotification("Workflow execution timeout", attempt.getId());
            }
        }
    }

    private void enforceTaskTTLs()
    {
        Instant startDeadline = ssm.getStoreTime().minus(taskTTL);

        List<TaskAttemptSummary> expiredTasks = ssm.findTasksStartedBeforeWithState(TaskStateCode.notDoneStates(), startDeadline, (long) 0, 100);

        Map<Long, List<TaskAttemptSummary>> attempts = expiredTasks.stream()
                .collect(groupingBy(TaskAttemptSummary::getAttemptId));

        for (Map.Entry<Long, List<TaskAttemptSummary>> entry : attempts.entrySet()) {
            logger.info("Task(s) timed out, canceling Session Attempt: {}, tasks={}", entry.getKey(), entry.getValue());
            boolean canceled = ssm.requestCancelAttempt(entry.getKey());
            String taskIds = entry.getValue().stream().mapToLong(TaskAttemptSummary::getId).mapToObj(Long::toString).collect(joining(","));
            if (canceled) {
                sendTimeoutNotification("Task execution timeout: " + taskIds, entry.getKey());
            }
        }
    }

    private void sendTimeoutNotification(String message, long attemptId)
    {
        StoredSessionAttemptWithSession attempt;
        try {
            attempt = ssm.getAttemptWithSessionById(attemptId);
        }
        catch (ResourceNotFoundException e) {
            logger.error("Session Attempt not found, ignoring: {}", attemptId);
            return;
        }

        int projectId = attempt.getSession().getProjectId();
        StoredProject project;
        try {
            project = psm.getProjectByIdInternal(projectId);
        }
        catch (ResourceNotFoundException e) {
            logger.error("Session Attempt not found, ignoring: {}", attemptId);
            return;
        }

        Optional<Long> wfId = attempt.getWorkflowDefinitionId();

        Optional<StoredWorkflowDefinitionWithProject> workflow = Optional.absent();
        if (wfId.isPresent()) {
            try {
                workflow = Optional.of(psm.getWorkflowDetailsById(wfId.get()));
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
                .build();

        try {
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
