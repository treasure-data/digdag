package io.digdag.core;

import java.util.Date;
import java.util.TimeZone;

public interface Scheduler
{
    TimeZone getTimeZone();

    ScheduleTime getFirstScheduleTime(Date currentTime);

    ScheduleTime nextScheduleTime(Date lastScheduleTime);
}
