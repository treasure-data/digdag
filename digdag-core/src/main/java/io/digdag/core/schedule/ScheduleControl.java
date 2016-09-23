package io.digdag.core.schedule;

import java.time.Instant;
import io.digdag.spi.ScheduleTime;
import io.digdag.core.repository.ResourceNotFoundException;

public class ScheduleControl
{
    private final ScheduleControlStore store;
    private StoredSchedule schedule;

    public ScheduleControl(ScheduleControlStore store, StoredSchedule schedule)
    {
        this.store = store;
        this.schedule = schedule;
    }

    public StoredSchedule get()
    {
        return schedule;
    }

    public void updateNextScheduleTime(ScheduleTime nextTime)
        throws ResourceNotFoundException
    {
        store.updateNextScheduleTime(schedule.getId(), nextTime);
    }

    public void updateNextScheduleTimeAndLastSessionTime(ScheduleTime nextTime, Instant lastSessionTime)
        throws ResourceNotFoundException
    {
        store.updateNextScheduleTimeAndLastSessionTime(schedule.getId(), nextTime, lastSessionTime);
    }

    public void enableSchedule()
    {
        store.enableSchedule(schedule.getId());
        schedule = store.getScheduleById(schedule.getId());
    }

    public void disableSchedule()
    {
        store.disableSchedule(schedule.getId());
        schedule = store.getScheduleById(schedule.getId());
    }
}
