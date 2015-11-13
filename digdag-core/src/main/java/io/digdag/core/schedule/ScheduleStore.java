package io.digdag.core.schedule;

import java.util.List;
import com.google.common.base.Optional;

public interface ScheduleStore
{
    List<StoredSchedule> getAllSchedules();  // TODO only for testing

    List<StoredSchedule> getSchedules(int pageSize, Optional<Long> lastId);

    StoredSchedule getScheduleById(long schedId);

    List<StoredSchedule> syncRepositorySchedules(int syncRepoId, List<Schedule> schedules);
}
