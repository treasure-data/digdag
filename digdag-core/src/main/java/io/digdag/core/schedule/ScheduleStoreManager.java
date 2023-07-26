package io.digdag.core.schedule;

import java.time.Instant;
import java.util.List;
import com.google.common.base.Optional;
import io.digdag.spi.AccountRouting;
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

    int lockReadySchedules(Instant currentTime, int limit, AccountRouting accountRouting, ScheduleAction func);
}
