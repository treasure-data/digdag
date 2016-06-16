package io.digdag.core.schedule;

import java.util.List;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.Instant;

import javax.annotation.PreDestroy;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.core.session.Session;
import io.digdag.core.session.SessionStateFlags;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.ImmutableStoredSessionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Locale.ENGLISH;

public class ScheduleExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleExecutor.class);

    private final ProjectStoreManager rm;
    private final ScheduleStoreManager sm;
    private final SchedulerManager srm;
    private final ScheduleHandler handler;
    private final SessionStoreManager sessionStoreManager;  // used for validation at backfill
    private final ScheduledExecutorService executor;

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
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("scheduler-%d")
                .build()
                );
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdown();
        // TODO wait for shutdown completion?
    }

    public void start()
    {
        executor.scheduleWithFixedDelay(() -> run(),
                1, 1, TimeUnit.SECONDS);
    }

    public void run()
    {
        try {
            sm.lockReadySchedules(Instant.now(), (store, storedSchedule) -> {
                schedule(new ScheduleControl(store, storedSchedule));
            });
        }
        catch (Throwable t) {
            logger.error("An uncaught exception is ignored. Scheduling will be retried.", t);
        }
    }

    public boolean schedule(ScheduleControl lockedSched)
    {
        StoredSchedule sched = lockedSched.get();

        // TODO If a workflow has wait-until-last-schedule attribute, don't start
        //      new session and return a ScheduleTime with delayed nextRunTime and
        //      same nextScheduleTime
        try {
            StoredWorkflowDefinitionWithProject def = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());

            Scheduler sr = srm.getScheduler(def);

            try {
                ScheduleTime nextTime = startSchedule(sched, sr, def);
                return lockedSched.tryUpdateNextScheduleTimeAndLastSessionTime(nextTime, sched.getNextScheduleTime());
            }
            catch (ResourceConflictException ex) {
                Exception error = new IllegalStateException("Detected duplicated excution of a scheduled workflow for the same scheduling time.", ex);
                logger.error("Database state error during scheduling. Skipping this schedule: {}", sched, error);
                ScheduleTime nextTime = sr.nextScheduleTime(sched.getNextScheduleTime());
                return lockedSched.tryUpdateNextScheduleTime(nextTime);
            }
            catch (RuntimeException ex) {
                logger.error("Error during scheduling. Pending this schedule for 1 hour: {}", sched, ex);
                ScheduleTime nextTime = ScheduleTime.of(
                        sched.getNextScheduleTime(),
                        sched.getNextRunTime().plusSeconds(3600));
                return lockedSched.tryUpdateNextScheduleTime(nextTime);
            }
        }
        catch (ResourceNotFoundException ex) {
            Exception error = new IllegalStateException("Workflow for a schedule id=" + sched.getId() + " is scheduled but does not exist.", ex);
            logger.error("Database state error during scheduling. Pending this schedule for 1 hour: {}", sched, error);
            ScheduleTime nextTime = ScheduleTime.of(
                    sched.getNextScheduleTime(),
                    sched.getNextRunTime().plusSeconds(3600));
            return lockedSched.tryUpdateNextScheduleTime(nextTime);
        }
    }

    private ScheduleTime startSchedule(StoredSchedule sched, Scheduler sr,
            StoredWorkflowDefinitionWithProject def)
        throws ResourceNotFoundException, ResourceConflictException
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

    public StoredSchedule skipScheduleToTime(int siteId, int schedId, Instant nextTime, Optional<Instant> runTime, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException
    {
        sm.getScheduleStore(siteId).getScheduleById(schedId); // validastes siteId

        return sm.lockScheduleById(schedId, (store, sched) -> {
            ScheduleControl lockedSched = new ScheduleControl(store, sched);

            Scheduler sr = getSchedulerOfSchedule(sched);

            ScheduleTime alignedNextTime = sr.getFirstScheduleTime(nextTime);
            if (sched.getNextScheduleTime().isBefore(alignedNextTime.getTime())) {
                // OK
                if (runTime.isPresent()) {
                    alignedNextTime = ScheduleTime.of(alignedNextTime.getTime(), runTime.get());
                }

                if (dryRun) {
                    sched = ImmutableStoredSchedule.builder()
                        .from(sched)
                        .nextRunTime(alignedNextTime.getRunTime())
                        .nextScheduleTime(alignedNextTime.getTime())
                        .build();
                }
                else {
                    // TODO validate return value is true, otherwise throw ResourceConflictException
                    sched = lockedSched.updateNextScheduleTime(alignedNextTime);
                }
                return sched;  // return updated StoredSchedule
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
        sm.getScheduleStore(siteId).getScheduleById(schedId); // validastes siteId

        return sm.lockScheduleById(schedId, (store, sched) -> {
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

                if (dryRun) {
                    sched = ImmutableStoredSchedule.builder()
                        .from(sched)
                        .nextRunTime(time.getRunTime())
                        .nextScheduleTime(time.getTime())
                        .build();
                }
                else {
                    // TODO validate return value is true, otherwise throw ResourceConflictException
                    sched = lockedSched.updateNextScheduleTime(time);
                }
                return sched;  // return updated StoredSchedule
            }
            else {
                // NG
                throw new ResourceConflictException("Specified time to skip schedules is already past");
            }
        });
    }

    private Scheduler getSchedulerOfSchedule(StoredSchedule sched)
        throws ResourceNotFoundException
    {
        StoredWorkflowDefinitionWithProject def = rm.getWorkflowDetailsById(sched.getWorkflowDefinitionId());
        return srm.getScheduler(def);
    }

    public List<StoredSessionAttemptWithSession> backfill(int siteId, int schedId, Instant fromTime, String attemptName, Optional<Integer> count, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException
    {
        sm.getScheduleStore(siteId).getScheduleById(schedId); // validastes siteId

        SessionStore ss = sessionStoreManager.getSessionStore(siteId);

        return sm.lockScheduleById(schedId, (store, sched) -> {
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
                                    .id(0L)
                                    .params(def.getConfig().getFactory().create())
                                    .stateFlags(SessionStateFlags.empty())
                                    .sessionId(0L)
                                    .createdAt(Instant.now())
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
