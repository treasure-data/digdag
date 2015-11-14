package io.digdag.core.schedule;

import java.util.Date;
import java.util.List;
import io.digdag.core.spi.ScheduleTime;
import io.digdag.core.repository.ResourceConflictException;

public interface ScheduleStoreManager
{
    ScheduleStore getScheduleStore(int siteId);

    interface ScheduleAction
    {
        ScheduleTime schedule(StoredSchedule schedule);
    }

    void lockReadySchedules(Date currentTime, ScheduleAction func);

    List<StoredSchedule> syncRepositorySchedules(int syncRepoId, List<Schedule> schedules)
        throws ResourceConflictException;
}
