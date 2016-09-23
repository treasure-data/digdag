package io.digdag.core.schedule;

import java.time.Instant;
import io.digdag.spi.ScheduleTime;
import io.digdag.core.repository.ResourceNotFoundException;

public interface ScheduleControlStore
{
    void updateNextScheduleTime(int schedId, ScheduleTime nextTime)
        throws ResourceNotFoundException;

    void updateNextScheduleTimeAndLastSessionTime(int schedId, ScheduleTime nextTime, Instant lastSessionTime)
        throws ResourceNotFoundException;
}
