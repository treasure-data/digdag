package io.digdag.core.schedule;

import java.util.List;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.ErrorReporter;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.core.BackgroundExecutor;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.workflow.AttemptLimitExceededException;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.core.session.Session;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.ImmutableStoredSessionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Locale.ENGLISH;

public class ScheduleExecutor
        implements BackgroundExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleExecutor.class);

    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final SchedulerManager srm;
    private final ScheduleHandler handler;
    private final SessionStoreManager sessionStoreManager;  // used for validation in backfill method
    private ScheduledExecutorService executor;

    @Inject(optional = true)
    private ErrorReporter errorReporter = ErrorReporter.empty();

    @Inject
    public ScheduleExecutor(
            ProjectStoreManager rm,
            ScheduleStoreManager sm,
            SchedulerManager srm,
            ScheduleHandler handler,
            SessionStoreManager sessionStoreManager)
    {
        this.rm = rm;
        this.sm = sm;
        this.srm = srm;
        this.handler = handler;
        this.sessionStoreManager = sessionStoreManager;
    }

    @PostConstruct
    public synchronized void start()
    {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("scheduler-%d")
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
            sm.lockReadySchedules(Instant.now(), (store, storedSchedule) -> {
                runSchedule(new ScheduleControl(store, storedSchedule));
            });
        }
        catch (Throwable t) {
            logger.error("An uncaught exception is ignored. Scheduling will be retried.", t);
            errorReporter.reportUncaughtError(t);
        }
    }

    private void runSchedule(ScheduleControl lockedSched)
    {
        StoredSchedule sched = lockedSched.get();

        // TODO If a workflow has wait-until-last-schedule attribute, don't start
        //      new session and return a ScheduleTime with delayed nextRunTime and
        //      same nextScheduleTime

        ScheduleTime nextSchedule;
        Instant successfulSessionTime = null;

        try {
            StoredWorkflowDefinitionWithProject def = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());
            Scheduler sr = srm.getScheduler(def);
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

    private ScheduleTime startSchedule(StoredSchedule sched, Scheduler sr,
            StoredWorkflowDefinitionWithProject def)
        throws ResourceNotFoundException, ResourceConflictException, ResourceLimitExceededException
    {
        Instant scheduleTime = sched.getNextScheduleTime();
        Instant runTime = sched.getNextRunTime();

        try {
            handler.start(def,
                    ScheduleTime.of(scheduleTime, runTime),
                    Optional.absent());
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
            ScheduleControl lockedSched = new ScheduleControl(store, sched);

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
            ImmutableList.Builder<StoredSessionAttemptWithSession> attempts = ImmutableList.builder();
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
                    try {
                        StoredSessionAttemptWithSession attempt = handler.start(def,
                                ScheduleTime.of(instant, sched.getNextScheduleTime()),
                                Optional.of(attemptName));
                        attempts.add(attempt);
                        // TODO this may throw ResourceLimitExceededException. But some sessions are already committed. To be able to rollback everything, inserting all sessions needs to be in a single transaction.
                    }
                    catch (SessionAttemptConflictException ex) {
                        // ignore because above start already committed other attempts. here can't rollback.
                        logger.warn("Session attempt conflicted after validation", ex);
                    }
                }
            }
            return attempts.build();
        });
    }
}
