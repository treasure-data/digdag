package io.digdag.core.schedule;

import io.digdag.spi.ScheduleTime;

public interface ScheduleControlStore
{
    boolean updateNextScheduleTime(long schedId, ScheduleTime nextTime);
}
