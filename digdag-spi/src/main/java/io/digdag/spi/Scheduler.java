package io.digdag.spi;

import java.time.Instant;
import java.time.ZoneId;

public interface Scheduler
{
    ZoneId getTimeZone();

    ScheduleTime getFirstScheduleTime(Instant currentTime);

    ScheduleTime nextScheduleTime(Instant lastScheduleTime);
}
