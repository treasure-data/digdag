package io.digdag.core.schedule;

import java.time.Instant;
import io.digdag.spi.ScheduleTime;

public interface ScheduleControlStore
{
    boolean updateNextScheduleTime(long schedId, ScheduleTime nextTime, Instant lastSessionTime);

    boolean updateNextScheduleTime(long schedId, ScheduleTime nextTime);
}
