package io.digdag.core;

import java.util.Date;

public interface ScheduleStoreManager
{
    ScheduleStore getScheduleStore(int siteId);

    interface ScheduleAction
    {
        ScheduleTime schedule(StoredSchedule schedule);
    }

    void lockReadySchedules(Date currentTime, ScheduleAction func);
}
