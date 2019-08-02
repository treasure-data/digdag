package io.digdag.core.schedule;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.BackgroundExecutor;
import io.digdag.core.ErrorReporter;
import io.digdag.core.database.TransactionManager;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.workflow.AttemptBuilder;
import io.digdag.core.workflow.AttemptLimitExceededException;
import io.digdag.core.workflow.AttemptRequest;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.core.workflow.TaskLimitExceededException;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.session.DelayedAttemptControlStore;
import io.digdag.core.session.Session;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.session.ImmutableStoredSessionAttempt;
import io.digdag.core.session.Session;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredDelayedSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.core.session.ImmutableStoredSessionAttempt;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.metrics.DigdagMetrics;
import static io.digdag.spi.metrics.DigdagMetrics.Category;
import io.digdag.util.DurationParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Locale.ENGLISH;

public class ScheduleExecutor
        implements BackgroundExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleExecutor.class);

    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final SchedulerManager srm;
    private final TransactionManager tm;
    private final SessionStoreManager sessionStoreManager;  // used for validation in backfill method
    private final AttemptBuilder attemptBuilder;
    private final WorkflowExecutor workflowExecutor;
    private final ConfigFactory cf;
    private final ScheduleConfig scheduleConfig;
    private ScheduledExecutorService executor;

    @Inject(optional = true)
    private ErrorReporter errorReporter = ErrorReporter.empty();

    @Inject
    private DigdagMetrics metrics;

    @Inject
    public ScheduleExecutor(
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            SchedulerManager srm,
            TransactionManager tm,
            SessionStoreManager sessionStoreManager,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor workflowExecutor,
            ConfigFactory cf,
            ScheduleConfig scheduleConfig)
    {
        this.rm = rm;
        this.sm = sm;
        this.srm = srm;
        this.tm = tm;
        this.sessionStoreManager = sessionStoreManager;
        this.attemptBuilder = attemptBuilder;
        this.workflowExecutor = workflowExecutor;
        this.cf = cf;
        this.scheduleConfig = scheduleConfig;
    }

    @VisibleForTesting
    boolean isStarted()
    {
        return executor != null;
    }

    @PostConstruct
    public synchronized void start()
    {
        if (scheduleConfig.getEnabled()) {
            if (executor == null) {
                executor = Executors.newScheduledThreadPool(1,
                        new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("scheduler-%d")
                        .build()
                        );
            }
            // TODO make interval configurable?
            executor.scheduleWithFixedDelay(() -> runSchedules(),
                    1, 1, TimeUnit.SECONDS);
            // TODO make interval configurable?
            executor.scheduleWithFixedDelay(() -> runDelayedAttempts(),
                    1, 1, TimeUnit.SECONDS);
        } else {
            logger.debug("Scheduler is disabled.");
        }
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

    private void runSchedules()
    {
        runSchedules(Instant.now());
    }

    private void runSchedules(Instant now)
    {
        try {
            while (runScheduleOnce(now))
                ;  // repeat while some schedules ready
        }
        catch (Throwable t) {
            logger.error("An uncaught exception is ignored. Scheduling will be retried.", t);
            errorReporter.reportUncaughtError(t);
            metrics.increment(Category.DEFAULT, "uncaughtErrors");
        }
    }

    @VisibleForTesting
    boolean runScheduleOnce(Instant now)
    {
        int count = tm.begin(() -> {
            // here uses limit=1 because selecting multiple rows with FOR UPDATE
            // has risk of too often deadlock.
            return sm.lockReadySchedules(now, 1, (store, storedSchedule) -> {
                runSchedule(new ScheduleControl(store, storedSchedule), now);
            });
        });
        return count > 0;
    }

    private void runDelayedAttempts()
    {
        runDelayedAttempts(Instant.now());
    }

    @VisibleForTesting
    void runDelayedAttempts(Instant now)
    {
        try {
            tm.begin(() -> {
                sessionStoreManager.lockReadyDelayedAttempts(now, (delayedAttemptControlStore, delayedAttempt) -> {
                    runDelayedAttempt(delayedAttemptControlStore, delayedAttempt);
                });
                return null;
            });
        }
        catch (Throwable t) {
            logger.error("An uncaught exception is ignored. Submitting delayed attempts will be retried.", t);
            errorReporter.reportUncaughtError(t);
            metrics.increment(Category.DEFAULT, "uncaughtErrors");
        }
    }

    private void runSchedule(ScheduleControl lockedSched, Instant now)
    {
        StoredSchedule sched = lockedSched.get();

        // TODO If a workflow has wait-until-last-schedule attribute, don't start
        //      new session and return a ScheduleTime with delayed nextRunTime and
        //      same nextScheduleTime

        ScheduleTime nextSchedule;
        Instant successfulSessionTime = null;

        try {
            StoredWorkflowDefinitionWithProject def = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());

            SessionStore ss = sessionStoreManager.getSessionStore(def.getProject().getSiteId());
            List<StoredSessionAttemptWithSession> activeAttempts = ss.getActiveAttemptsOfWorkflow(def.getProject().getId(), def.getName(), 1, Optional.absent());

            Scheduler sr = srm.getScheduler(def);

            Config scheduleConfig = SchedulerManager.getScheduleConfig(def);
            boolean skipOnOvertime = scheduleConfig.get("skip_on_overtime", boolean.class, false);
            Optional<DurationParam> skipDelay = scheduleConfig.getOptional("skip_delayed_by", DurationParam.class);

            // task should run at scheduled time within skipDelay.
            if (skipDelay.isPresent() && now.isAfter(sched.getNextRunTime().plusSeconds(skipDelay.get().getDuration().getSeconds()))) {
                logger.info("Now={} is too late from scheduled time={}. It's over skip_delayed_by={}. Skipping this schedule: {}", now, sched.getNextScheduleTime(), skipDelay.get(), sched);
                nextSchedule = sr.nextScheduleTime(sched.getNextScheduleTime());
            }
            else if (!activeAttempts.isEmpty() && skipOnOvertime) {
                logger.info("An attempt of the scheduled workflow is still running and skip_on_overtime = true. Skipping this schedule: {}", sched);
                nextSchedule = sr.nextScheduleTime(sched.getNextScheduleTime());
            }
            else {
                try {
                    nextSchedule = startSchedule(sched, sr, def);
                    successfulSessionTime = sched.getNextScheduleTime();
                }
                catch (ResourceLimitExceededException ex) {
                    logger.info("Number of attempts or tasks exceed limit. Pending this schedule for 10 minutes: {}", sched, ex);
                    nextSchedule = ScheduleTime.of(
                            sched.getNextScheduleTime(),
                            ScheduleTime.alignedNow().plusSeconds(600));
                }
                catch (ResourceConflictException ex) {
                    Exception error = new IllegalStateException("Detected duplicated excution of a scheduled workflow for the same scheduling time.", ex);
                    logger.error("Database state error during scheduling. Skipping this schedule: {}", sched, error);
                    nextSchedule = sr.nextScheduleTime(sched.getNextScheduleTime());
                }
            }
        }
        catch (ResourceNotFoundException ex) {
            logger.error("Database state error during scheduling. Pending this schedule for 1 hour: {}", sched, ex);
            nextSchedule = ScheduleTime.of(
                    sched.getNextScheduleTime(),
                    sched.getNextRunTime().plusSeconds(3600));
        }
        catch (RuntimeException ex) {
            logger.error("Error during scheduling. Pending this schedule for 1 hour: {}", sched, ex);
            nextSchedule = ScheduleTime.of(
                    sched.getNextScheduleTime(),
                    ScheduleTime.alignedNow().plusSeconds(3600));
        }

        try {
            logger.info("Updating next schedule time: sched={}, next={}, lastSessionTime={}", sched, nextSchedule, successfulSessionTime);
            if (successfulSessionTime != null) {
                lockedSched.updateNextScheduleTimeAndLastSessionTime(nextSchedule, successfulSessionTime);
            }
            else {
                lockedSched.updateNextScheduleTime(nextSchedule);
            }
        }
        catch (ResourceNotFoundException ex) {
            throw new IllegalStateException("Workflow for a schedule id=" + sched.getId() + " is scheduled but does not exist.", ex);
        }
    }

    @VisibleForTesting
    ScheduleTime startSchedule(StoredSchedule sched, Scheduler sr,
            StoredWorkflowDefinitionWithProject def)
        throws ResourceNotFoundException, ResourceConflictException, ResourceLimitExceededException
    {
        Instant scheduleTime = sched.getNextScheduleTime();
        Instant runTime = sched.getNextRunTime();

        AttemptRequest ar = newAttemptRequest(
                def, ScheduleTime.of(scheduleTime, runTime),
                Optional.absent(), sched.getLastSessionTime());
        try {
            workflowExecutor.submitWorkflow(def.getProject().getSiteId(),
                    ar, def);
        }
        catch (SessionAttemptConflictException ex) {
            logger.debug("Scheduled attempt {} is already executed. Skipping", ex.getConflictedSession());
        }
        return sr.nextScheduleTime(scheduleTime);
    }

    ////
    // See io.digdag.core.repository.ProjectControl.updateSchedules
    // for updating schedules when a new revision is uploaded
    //

    public StoredSchedule skipScheduleToTime(int siteId, int schedId, Instant nextTime, Optional<Instant> runTime, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException
    {
        return sm.getScheduleStore(siteId).updateScheduleById(schedId, (store, sched) -> {
            ScheduleControl lockedSched = new ScheduleControl(store, sched);

            Scheduler sr = getSchedulerOfSchedule(sched);

            ScheduleTime alignedNextTime = sr.getFirstScheduleTime(nextTime);
            if (sched.getNextScheduleTime().isBefore(alignedNextTime.getTime())) {
                // OK
                if (runTime.isPresent()) {
                    alignedNextTime = ScheduleTime.of(alignedNextTime.getTime(), runTime.get());
                }

                StoredSchedule updatedSched = copyWithUpdatedScheduleTime(sched, alignedNextTime);
                if (!dryRun) {
                    lockedSched.updateNextScheduleTime(alignedNextTime);
                }
                return updatedSched;
            }
            else {
                // NG
                throw new ResourceConflictException("Specified time to skip schedules is already past");
            }
        });
    }

    public StoredSchedule skipScheduleByCount(int siteId, int schedId, Instant currentTime, int count, Optional<Instant> runTime, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException
    {
        return sm.getScheduleStore(siteId).updateScheduleById(schedId, (store, sched) -> {
            ScheduleControl lockedSched = new ScheduleControl(store, sched);

            Scheduler sr = getSchedulerOfSchedule(sched);

            ScheduleTime time = sr.getFirstScheduleTime(currentTime);
            for (int i=0; i < count; i++) {
                time = sr.nextScheduleTime(time.getTime());
            }
            if (sched.getNextScheduleTime().isBefore(time.getTime())) {
                // OK
                if (runTime.isPresent()) {
                    time = ScheduleTime.of(time.getTime(), runTime.get());
                }

                StoredSchedule updatedSched = copyWithUpdatedScheduleTime(sched, time);
                if (!dryRun) {
                    lockedSched.updateNextScheduleTime(time);
                }
                return updatedSched;
            }
            else {
                // NG
                throw new ResourceConflictException("Specified time to skip schedules is already past");
            }
        });
    }

    private static StoredSchedule copyWithUpdatedScheduleTime(StoredSchedule sched, ScheduleTime nextTime)
    {
        return ImmutableStoredSchedule.builder()
            .from(sched)
            .nextRunTime(nextTime.getRunTime())
            .nextScheduleTime(nextTime.getTime())
            .build();
    }

    private Scheduler getSchedulerOfSchedule(StoredSchedule sched)
        throws ResourceNotFoundException
    {
        StoredWorkflowDefinitionWithProject def = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());
        return srm.getScheduler(def);
    }

    public List<StoredSessionAttemptWithSession> backfill(int siteId, int schedId, Instant fromTime, String attemptName, Optional<Integer> count, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException, ResourceLimitExceededException
    {
        SessionStore ss = sessionStoreManager.getSessionStore(siteId);

        return sm.getScheduleStore(siteId).lockScheduleById(schedId, (store, sched) -> {
            StoredWorkflowDefinitionWithProject def = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());
            Scheduler sr = srm.getScheduler(def);

            boolean useCount = count.isPresent();
            int remaining = count.or(0);

            List<Instant> instants = new ArrayList<>();
            Instant time = sr.getFirstScheduleTime(fromTime).getTime();
            while (time.isBefore(sched.getNextScheduleTime())) {
                if (useCount) {
                    if (remaining <= 0) {
                        break;
                    }
                    remaining--;
                }
                instants.add(time);
                time = sr.nextScheduleTime(time).getTime();
            }

            if (useCount && remaining > 0) {
                throw new IllegalArgumentException(String.format(ENGLISH,
                        "count is set to %d but there are only %d attempts until the next schedule time",
                        count.get(), count.get() - remaining));
            }

            // confirm sessions with the same attemptName doesn't exist
            for (Instant instant : instants) {
                try {
                    ss.getAttemptByName(def.getProject().getId(), def.getName(), instant, attemptName);
                    throw new ResourceConflictException(String.format(Locale.ENGLISH,
                                "Attempt of project id=%d workflow=%s instant=%s attempt name=%s already exists",
                                def.getProject().getId(), def.getName(), instant, attemptName));
                }
                catch (ResourceNotFoundException ex) {
                    // OK
                }
            }

            // run sessions
            return workflowExecutor.submitTransaction(siteId, (submitter) -> {
                ImmutableList.Builder<StoredSessionAttemptWithSession> attempts = ImmutableList.builder();

                Optional<StoredSessionAttemptWithSession> lastAttempt = Optional.absent();

                for (Instant instant : instants) {
                    if (dryRun) {
                        attempts.add(
                                StoredSessionAttemptWithSession.dryRunDummy(siteId,
                                    Session.of(def.getProject().getId(), def.getName(), instant),
                                    ImmutableStoredSessionAttempt.builder()
                                        .retryAttemptName(Optional.of(attemptName))
                                        .workflowDefinitionId(Optional.of(def.getId()))
                                        .timeZone(def.getTimeZone())
                                        .id(0L)
                                        .index(0)
                                        .params(def.getConfig().getFactory().create())
                                        .stateFlags(AttemptStateFlags.empty())
                                        .sessionId(0L)
                                        .createdAt(Instant.now())
                                        .finishedAt(Optional.absent())
                                        .build()
                                )
                            );
                    }
                    else {
                        Optional<Instant> lastExecutedSessionTime =
                            lastAttempt
                            .transform(a -> a.getSession().getSessionTime());
                        if (!lastExecutedSessionTime.isPresent()) {
                            lastExecutedSessionTime = submitter.getLastExecutedSessionTime(
                                    sched.getProjectId(), sched.getWorkflowName(), instant);
                        }
                        AttemptRequest ar = newAttemptRequest(
                                def, ScheduleTime.of(instant, sched.getNextScheduleTime()),
                                Optional.of(attemptName), lastExecutedSessionTime);
                        StoredSessionAttemptWithSession attempt =
                            submitter.submitDelayedAttempt(ar, lastAttempt.transform(a -> a.getSessionId()));
                        lastAttempt = Optional.of(attempt);
                        attempts.add(attempt);
                    }
                }

                return attempts.build();
            });
        });
    }

    public void runDelayedAttempt(DelayedAttemptControlStore control, StoredDelayedSessionAttempt delayedAttempt)
    {
        try {
            control.lockSessionOfAttempt(delayedAttempt.getAttemptId(), (sessionControlStore, storedAttemptWithSession) -> {
                if (!storedAttemptWithSession.getWorkflowDefinitionId().isPresent()) {
                    throw new ResourceNotFoundException("Delayed attempt must have a stored workflow");
                }
                WorkflowDefinition def = rm.getProjectStore(storedAttemptWithSession.getSiteId())
                            .getWorkflowDefinitionById(storedAttemptWithSession.getWorkflowDefinitionId().get());
                workflowExecutor.storeTasks(
                        sessionControlStore,
                        storedAttemptWithSession,
                        def,
                        ImmutableList.of(),
                        ImmutableList.of());
                return true;
            });
        }
        catch (ResourceConflictException ex) {
            logger.warn("Delayed attempt conflicted: {}", delayedAttempt, ex);
        }
        catch (ResourceNotFoundException ex) {
            logger.warn("Invalid delayed attempt: {}", delayedAttempt, ex);
        }
        catch (ResourceLimitExceededException ex) {
            tm.reset();
            logger.warn("Failed to start delayed attempt Due to too many active tasks. Will be retried after 5 minutes.", ex);
            control.delayDelayedAttempt(delayedAttempt.getAttemptId(), Instant.now().plusSeconds(5 * 60));
            return;
        }
        control.completeDelayedAttempt(delayedAttempt.getAttemptId());
    }

    private AttemptRequest newAttemptRequest(
            StoredWorkflowDefinitionWithProject def,
            ScheduleTime time, Optional<String> retryAttemptName,
            Optional<Instant> lastExecutedSessionTime)
    {
        return attemptBuilder.buildFromStoredWorkflow(
                def,
                cf.create(),
                time,
                retryAttemptName,
                Optional.absent(),
                ImmutableList.of(),
                lastExecutedSessionTime);
    }
}
