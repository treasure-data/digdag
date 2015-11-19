package io.digdag.core.schedule;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.spi.ScheduleTime;
import io.digdag.core.spi.Scheduler;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleExecutor.class);

    private final ExecutorService executor;
    private final ScheduleStoreManager sm;
    private final SchedulerManager scheds;
    private final ScheduleStarter starter;

    @Inject
    public ScheduleExecutor(ScheduleStoreManager sm, SchedulerManager scheds,
            ScheduleStarter starter)
    {
        this.executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("scheduler-%d")
                .build()
                );
        this.sm = sm;
        this.scheds = scheds;
        this.starter = starter;
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

    public ScheduleTime schedule(StoredSchedule sched)
    {
        // TODO If a workflow has wait-until-lastl-schedule attribute, don't start
        //      new session and return a ScheduleTime with delayed nextRunTime and
        //      same nextScheduleTime
        Date scheduleTime = sched.getNextScheduleTime();
        Scheduler sr = scheds.getScheduler(sched.getConfig());
        Optional<String> from = sched.getConfig().getOptional("from", String.class);
        try {
            starter.start(sched.getWorkflowId(), from,
                    sr.getTimeZone(), ScheduleTime.of(sched.getNextRunTime(), scheduleTime));
            return sr.nextScheduleTime(scheduleTime);
        }
        catch (ResourceNotFoundException ex) {
            Exception error = new IllegalStateException("Schedule for a workflow id="+sched.getWorkflowId()+" is scheduled but does not exist.", ex);
            logger.error("Database state error during scheduling. Pending this schedule for 1 hour", error);
            return ScheduleTime.of(
                    new Date(sched.getNextRunTime().getTime() + 3600*1000),
                    sched.getNextScheduleTime());
        }
        catch (ResourceConflictException ex) {
            Exception error = new IllegalStateException("Detected duplicated excution of a scheduled workflow for the same scheduling time.", ex);
            logger.error("Database state error during scheduling. Skipping this schedule", error);
            return sr.nextScheduleTime(scheduleTime);
        } catch (RuntimeException ex) {
            logger.error("Error during scheduling. Pending this schedule for 1 hour", ex);
            return ScheduleTime.of(
                    new Date(sched.getNextRunTime().getTime() + 3600*1000),
                    sched.getNextScheduleTime());
        }
    }
}
