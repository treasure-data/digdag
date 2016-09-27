package io.digdag.core.schedule;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceNotFoundException;

public interface ScheduleStore
{
    List<StoredSchedule> getSchedules(int pageSize, Optional<Integer> lastId);

    StoredSchedule getScheduleById(int schedId)
        throws ResourceNotFoundException;

    List<StoredSchedule> getSchedulesByProjectId(int projectId, int pageSize, Optional<Integer> lastId);

    StoredSchedule getScheduleByProjectIdAndWorkflowName(int projectId, String workflowName)
            throws ResourceNotFoundException;
}
