package io.digdag.core;

import java.util.Date;

public interface Scheduler
{
    Date getFirstScheduleTime(Date currentTime);

    Date nextScheduleTime(Date lastScheduleTime);
}
