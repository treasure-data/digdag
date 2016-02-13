package io.digdag.spi;

import java.time.Instant;
import java.time.ZoneId;

public interface Scheduler
{
    ZoneId getTimeZone();

    // align this time with the last schedule time.
    // returned value is same with currentTime or after currentTime.
    ScheduleTime getFirstScheduleTime(Instant currentTime);

    // align this time with the next schedule time.
    // returned value is after lastScheduleTime.
    ScheduleTime nextScheduleTime(Instant lastScheduleTime);

    // align this time with the last schedule time.
    // returned value is before currentScheduleTime.
    ScheduleTime lastScheduleTime(Instant currentScheduleTime);
}
