package io.digdag.core.schedule;

import java.time.Instant;
import io.digdag.spi.ScheduleTime;

public interface ScheduleControlStore
{
    boolean updateNextScheduleTime(long schedId, ScheduleTime nextTime, Instant lastSessionInstant);

    boolean updateNextScheduleTime(long schedId, ScheduleTime nextTime);
}
