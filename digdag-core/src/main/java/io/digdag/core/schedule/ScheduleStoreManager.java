package io.digdag.core.schedule;

import java.util.Date;
import java.util.List;
import com.google.common.base.Optional;
import io.digdag.spi.ScheduleTime;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public interface ScheduleStoreManager
{
    ScheduleStore getScheduleStore(int siteId);

    interface ScheduleAction
    {
        ScheduleTime schedule(StoredSchedule schedule);
    }

    void lockReadySchedules(Date currentTime, ScheduleAction func);

    interface ScheduleLockAction <T>
    {
        T call(ScheduleControl control);
    }

    <T> T lockScheduleById(long schedId, ScheduleLockAction<T> func)
        throws ResourceNotFoundException;
}
