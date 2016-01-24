package io.digdag.core.repository;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.schedule.Schedule;

public interface RepositoryControlStore
{
    List<StoredWorkflowDefinition> getWorkflowDefinitions(int revId, int pageSize, Optional<Integer> lastId);

    StoredRevision putRevision(int repoId, Revision revision);

    void insertRevisionArchiveData(int revId, byte[] data)
            throws ResourceConflictException;

    StoredWorkflowDefinition insertWorkflowDefinition(int repoId, int revId, WorkflowDefinition workflow)
        throws ResourceConflictException;

    void updateSchedules(int repoId, List<Schedule> schedules)
            throws ResourceConflictException;

    //void deleteRepository(int repoId);  // set deleted_at to repository, and delete schedules
}
