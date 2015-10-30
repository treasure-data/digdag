package io.digdag.core;

import java.util.Date;
import com.google.common.base.Optional;

public interface ScheduleStoreManager
{
    ScheduleStore getScheduleStore(int siteId);

    interface ScheduleAction
    {
        ScheduleTime schedule(StoredSchedule schedule);
    }

    void lockReadySchedules(Date currentTime, ScheduleAction func);
}
