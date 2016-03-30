package io.digdag.core.repository;

import java.util.List;
import java.time.ZoneId;
import com.google.common.base.Optional;
import io.digdag.core.schedule.Schedule;

public interface RepositoryControlStore
{
    StoredRevision insertRevision(int repoId, Revision revision)
        throws ResourceConflictException;

    void insertRevisionArchiveData(int revId, byte[] data)
            throws ResourceConflictException;

    StoredWorkflowDefinition insertWorkflowDefinition(int repoId, int revId, WorkflowDefinition workflow, ZoneId workflowTimeZone)
        throws ResourceConflictException;

    void updateSchedules(int repoId, List<Schedule> schedules)
            throws ResourceConflictException;

    //void deleteRepository(int repoId);  // set deleted_at to repository, and delete schedules
}
