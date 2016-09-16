package io.digdag.core.schedule;

import java.time.Instant;
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
        void schedule(ScheduleControlStore store, StoredSchedule schedule);
    }

    void lockReadySchedules(Instant currentTime, ScheduleAction func);

    interface ScheduleLockAction <T>
    {
        T call(ScheduleControlStore store, StoredSchedule storedSched)
            throws ResourceNotFoundException, ResourceConflictException;
    }

    <T> T lockScheduleById(int schedId, ScheduleLockAction<T> func)
        throws ResourceNotFoundException, ResourceConflictException;
}
