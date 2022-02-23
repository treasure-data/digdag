package io.digdag.spi;

import com.google.common.base.Optional;

import java.time.Instant;
import java.time.ZoneId;

public interface Scheduler
{
    ZoneId getTimeZone();

    Optional<Instant> getStartDate();

    Optional<Instant> getEndDate();

    // align given time with the last time of this schedule.
    // getRunTime of returned ScheduleTime is same or after currentTime.
    ScheduleTime getFirstScheduleTime(Instant currentTime);

    // align given time with the next schedule time.
    // getTime of returned ScheduleTime is after lastScheduleTime.
    ScheduleTime nextScheduleTime(Instant lastScheduleTime);

    // align given time with the last schedule time.
    // getTime of returned ScheduleTime is before currentScheduleTime.
    ScheduleTime lastScheduleTime(Instant currentScheduleTime);
}
