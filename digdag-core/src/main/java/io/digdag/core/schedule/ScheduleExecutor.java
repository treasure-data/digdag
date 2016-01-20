package io.digdag.core.schedule;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigException;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.core.workflow.SubtaskMatchPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleExecutor.class);

    private final ExecutorService executor;
    private final ScheduleStoreManager sm;
    private final SchedulerManager scheds;
    private final ScheduleHandler handler;

    @Inject
    public ScheduleExecutor(ScheduleStoreManager sm, SchedulerManager scheds,
            ScheduleHandler handler)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("scheduler-%d")
                .build()
                );
        this.sm = sm;
        this.scheds = scheds;
        this.handler = handler;
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
                sm.lockReadySchedules(new Date(), (storedSchedule) -> {
                    return schedule(storedSchedule);
                });
            }
        }
        catch (Throwable t) {
            logger.error("Uncaught exception", t);
        }
    }

    // used by RepositoryControl.syncLatestRevision and schedule
    public static TaskMatchPattern getScheduleWorkflowMatchPattern(Config scheduleConfig)
    {
        String pattern = scheduleConfig.get("trigger", String.class);
        TaskMatchPattern compiled = TaskMatchPattern.compile(pattern);
        if (compiled.getSubtaskMatchPattern().isPresent()) {
            throw new ConfigException("trigger: option doesn't accept subtask name");
        }
        return compiled;
    }

    public ScheduleTime schedule(StoredSchedule sched)
    {
        // TODO If a workflow has wait-until-last-schedule attribute, don't start
        //      new session and return a ScheduleTime with delayed nextRunTime and
        //      same nextScheduleTime
        Date scheduleTime = sched.getNextScheduleTime();
        Scheduler sr = scheds.getScheduler(sched.getConfig());
        Optional<SubtaskMatchPattern> subtaskMatchPattern = getScheduleWorkflowMatchPattern(sched.getConfig()).getSubtaskMatchPattern();
        try {
            handler.start(sched.getWorkflowSourceId(), subtaskMatchPattern,
                    sr.getTimeZone(), ScheduleTime.of(sched.getNextRunTime(), scheduleTime));
            return sr.nextScheduleTime(scheduleTime);
        }
        catch (ResourceNotFoundException ex) {
            Exception error = new IllegalStateException("Workflow for a schedule id="+sched.getId()+" is scheduled but does not exist.", ex);
            logger.error("Database state error during scheduling. Pending this schedule for 1 hour", error);
            return ScheduleTime.of(
                    new Date(sched.getNextRunTime().getTime() + 3600*1000),
                    sched.getNextScheduleTime());
        }
        catch (ResourceConflictException ex) {
            Exception error = new IllegalStateException("Detected duplicated excution of a scheduled workflow for the same scheduling time.", ex);
            logger.error("Database state error during scheduling. Skipping this schedule", error);
            return sr.nextScheduleTime(scheduleTime);
        }
        catch (RuntimeException ex) {
            logger.error("Error during scheduling. Pending this schedule for 1 hour", ex);
            return ScheduleTime.of(
                    new Date(sched.getNextRunTime().getTime() + 3600*1000),
                    sched.getNextScheduleTime());
        }
    }

    public StoredSchedule skipScheduleToTime(int siteId, long schedId, Date nextTime, Optional<Date> runTime, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException
    {
        sm.getScheduleStore(siteId).getScheduleById(schedId); // validastes siteId

        Optional<StoredSchedule> optional = sm.lockScheduleById(schedId, (ScheduleControl control) -> {
            StoredSchedule sched = control.getSchedule();
            Scheduler sr = scheds.getScheduler(sched.getConfig());

            ScheduleTime alignedNextTime = sr.getFirstScheduleTime(new Date(nextTime.getTime() - 1));
            if (sched.getNextScheduleTime().getTime() < alignedNextTime.getScheduleTime().getTime()) {
                // OK
                if (runTime.isPresent()) {
                    alignedNextTime = ScheduleTime.of(runTime.get(), alignedNextTime.getScheduleTime());
                }
                if (!dryRun) {
                    control.updateNextScheduleTime(alignedNextTime); // TODO validate return value is true, otherwise throw ResourceConflictException
                }
                return Optional.of(control.getSchedule());
            }
            else {
                // NG
                return Optional.<StoredSchedule>absent();
            }
        });
        if (optional.isPresent()) {
            return optional.get();
        }
        else {
            throw new ResourceConflictException("Specified time to skip schedules is already past");
        }
    }

    public StoredSchedule skipScheduleByCount(int siteId, long schedId, Date currentTime, int count, Optional<Date> runTime, boolean dryRun)
        throws ResourceNotFoundException, ResourceConflictException
    {
        sm.getScheduleStore(siteId).getScheduleById(schedId); // validastes siteId

        Optional<StoredSchedule> optional = sm.lockScheduleById(schedId, (ScheduleControl control) -> {
            StoredSchedule sched = control.getSchedule();
            Scheduler sr = scheds.getScheduler(sched.getConfig());

            ScheduleTime time = sr.getFirstScheduleTime(currentTime);
            for (int i=0; i < count; i++) {
                time = sr.nextScheduleTime(time.getScheduleTime());
            }
            if (sched.getNextScheduleTime().getTime() < time.getScheduleTime().getTime()) {
                // OK
                if (runTime.isPresent()) {
                    time = ScheduleTime.of(runTime.get(), time.getScheduleTime());
                }
                if (!dryRun) {
                    control.updateNextScheduleTime(time); // TODO validate return value is true, otherwise throw ResourceConflictException
                }
                return Optional.of(control.getSchedule());
            }
            else {
                // NG
                return Optional.<StoredSchedule>absent();
            }
        });
        if (optional.isPresent()) {
            return optional.get();
        }
        else {
            throw new ResourceConflictException("Specified time to skip schedules is already past");
        }
    }
}
