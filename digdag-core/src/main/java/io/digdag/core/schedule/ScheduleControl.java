package io.digdag.core.schedule;

import java.util.Date;
import io.digdag.spi.ScheduleTime;

public class ScheduleControl
{
    private final ScheduleControlStore store;
    private StoredSchedule schedule;

    public ScheduleControl(ScheduleControlStore store, StoredSchedule schedule)
    {
        this.store = store;
        this.schedule = schedule;
    }

    public StoredSchedule getSchedule()
    {
        return schedule;
    }

    public boolean updateNextScheduleTime(ScheduleTime nextTime)
    {
        if (store.updateNextScheduleTime(schedule.getId(), nextTime)) {
            this.schedule = ImmutableStoredSchedule.builder()
                .from(schedule)
                .nextRunTime(nextTime.getRunTime())
                .nextScheduleTime(nextTime.getScheduleTime())
                .build();
            return true;
        }
        else {
            return false;
        }
    }
}
