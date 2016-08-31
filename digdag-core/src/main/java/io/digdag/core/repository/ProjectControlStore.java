package io.digdag.core.repository;

import java.util.List;
import java.time.ZoneId;
import java.time.Instant;
import com.google.common.base.Optional;
import io.digdag.core.schedule.Schedule;
import io.digdag.core.schedule.ScheduleStatus;
import io.digdag.spi.ScheduleTime;

public interface ProjectControlStore
{
    StoredRevision insertRevision(int projId, Revision revision)
        throws ResourceConflictException;

    void insertRevisionArchiveData(int revId, byte[] data)
            throws ResourceConflictException;

    StoredWorkflowDefinition insertWorkflowDefinition(int projId, int revId, WorkflowDefinition workflow, ZoneId workflowTimeZone)
        throws ResourceConflictException;

    interface ScheduleUpdateAction
    {
        public ScheduleTime apply(ScheduleStatus oldStatus, Schedule newSchedule);
    }

    void updateSchedules(int projId, List<Schedule> schedules, ScheduleUpdateAction func)
        throws ResourceConflictException;

    void deleteSchedules(int projId);
}
