package io.digdag.core.schedule;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.spi.ac.AccessController;

public interface ScheduleStore
{
    List<StoredSchedule> getSchedules(int pageSize, Optional<Integer> lastId, AccessController.ListFilter acFilter);

    StoredSchedule getScheduleById(int schedId)
        throws ResourceNotFoundException;

    List<StoredSchedule> getSchedulesByProjectId(int projectId, int pageSize, Optional<Integer> lastId, AccessController.ListFilter acFilter);

    StoredSchedule getScheduleByProjectIdAndWorkflowName(int projectId, String workflowName)
            throws ResourceNotFoundException;

    interface ScheduleUpdateAction <T>
    {
        T call(ScheduleControlStore store, StoredSchedule storedSched)
            throws ResourceNotFoundException, ResourceConflictException;
    }

    <T> T updateScheduleById(int schedId, ScheduleUpdateAction<T> func)
        throws ResourceNotFoundException, ResourceConflictException;

    interface ScheduleLockAction <T>
    {
        T call(ScheduleControlStore store, StoredSchedule storedSched)
            throws ResourceNotFoundException, ResourceConflictException, ResourceLimitExceededException;
    }

    <T> T lockScheduleById(int schedId, ScheduleLockAction<T> func)
        throws ResourceNotFoundException, ResourceConflictException, ResourceLimitExceededException;
}
