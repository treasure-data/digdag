package io.digdag.core;

import java.util.List;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowScheduler
        implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(WorkflowScheduler.class);

    private final ScheduleStoreManager sm;
    private final SchedulerManager scheds;

    public WorkflowScheduler(ScheduleStoreManager sm, SchedulerManager scheds)
    {
        this.sm = sm;
        this.scheds = scheds;
    }

    @Override
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

    public Date schedule(StoredSchedule sched)
    {
        return scheds.getScheduler(sched.getConfig()).nextScheduleTime(sched.getNextScheduleTime());
    }
}
