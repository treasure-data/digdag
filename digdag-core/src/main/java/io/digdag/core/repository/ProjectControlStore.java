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

    public class ScheduleTimeWithInfo
    {
        final ScheduleTime scheduleTime;
        final boolean clearSchedule;

        ScheduleTimeWithInfo(ScheduleTime scheduleTime, boolean clearSchedule)
        {
            this.scheduleTime = scheduleTime;
            this.clearSchedule = clearSchedule;
        }

        public ScheduleTime getScheduleTime() {
            return scheduleTime;
        }

        public boolean isClearSchedule() {
            return clearSchedule;
        }

        public static ScheduleTimeWithInfo of(ScheduleTime scheduleTime)
        {
            return new ScheduleTimeWithInfo(scheduleTime, false);
        }

        public static ScheduleTimeWithInfo of(ScheduleTime scheduleTime, boolean clearSchedule)
        {
            return new ScheduleTimeWithInfo(scheduleTime, clearSchedule);
        }
    }

    interface ScheduleUpdateAction <T extends Schedule>
    {
        ScheduleTimeWithInfo apply(ScheduleStatus oldStatus, T newSchedule);
    }

    <T extends Schedule> void updateSchedules(int projId, List<T> schedules, ScheduleUpdateAction<T> func)
        throws ResourceConflictException;

    void deleteSchedules(int projId);
}
