package io.digdag.core.schedule;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceNotFoundException;

public interface ScheduleStore
{
    List<StoredSchedule> getSchedules(int pageSize, Optional<Long> lastId);

    StoredSchedule getScheduleById(long schedId)
        throws ResourceNotFoundException;
}
