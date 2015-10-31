package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduleExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleExecutor.class);

    private final ExecutorService executor;
    private final ScheduleStoreManager sm;
    private final SchedulerManager scheds;
    private final ScheduleStarter starter;
    private final SlaExecutor slaExecutor;

    @Inject
    public ScheduleExecutor(ScheduleStoreManager sm, SchedulerManager scheds,
            ScheduleStarter starter, SlaExecutor slaExecutor)
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
        this.slaExecutor = slaExecutor;
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
        ScheduleTime next;
        if (sched.getScheduleType().isSlaTask()) {
            next = slaExecutor.slaTrigger(sched);
        }
        else {
            Scheduler sr = scheds.getScheduler(sched.getConfig());
            starter.start(sched.getWorkflowId(), sr.getTimeZone(), scheduleTime);
            next = sr.nextScheduleTime(scheduleTime);
        }
        return next;
    }
}
