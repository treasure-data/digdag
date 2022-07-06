package io.digdag.standards.scheduler;

import io.digdag.spi.Scheduler;

import java.time.Instant;

public abstract class BaseScheduler implements Scheduler {
    Instant SCHEDULE_END = Instant.ofEpochSecond(253370764800L); // 9999-01-01 00:00:00 +0000

    boolean isScheduleFinished(Instant time)
    {
        if (time.equals(SCHEDULE_END) || time.isAfter(SCHEDULE_END)) {
            return true;
        }
        else {
            return false;
        }
    }
}
