package io.digdag.core;

import java.util.List;
import com.google.common.base.Optional;

public interface ScheduleStore
        extends Store
{
    List<StoredSchedule> getAllSchedules();  // TODO only for testing

    List<StoredSchedule> getSchedules(int pageSize, Optional<Long> lastId);

    List<StoredSchedule> syncRepositorySchedules(int repoId, List<Schedule> schedules);
}
