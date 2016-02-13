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

    public StoredSchedule updateNextScheduleTime(ScheduleTime nextTime)
        throws ResourceNotFoundException
    {
        if (store.updateNextScheduleTime(schedule.getId(), nextTime)) {
            return ImmutableStoredSchedule.builder()
                .from(schedule)
                .nextRunTime(nextTime.getRunTime())
                .nextScheduleTime(nextTime.getTime())
                .build();
        }
        else {
            throw new ResourceNotFoundException("schedule id=" + schedule.getId());
        }
    }

    public boolean tryUpdateNextScheduleTime(ScheduleTime nextTime)
    {
        return store.updateNextScheduleTime(schedule.getId(), nextTime);
    }

    public boolean tryUpdateNextScheduleTimeAndLastSessionInstant(ScheduleTime nextTime, Instant lastSessionInstant)
    {
        return store.updateNextScheduleTime(schedule.getId(), nextTime, lastSessionInstant);
    }
}
