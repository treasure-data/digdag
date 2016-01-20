package io.digdag.core.repository;

import java.util.List;
import com.google.common.base.Optional;
import io.digdag.core.schedule.Schedule;

public interface RepositoryControlStore
{
    List<StoredWorkflowSource> getWorkflowSources(int revId, int pageSize, Optional<Integer> lastId);

    StoredRevision putRevision(int repoId, Revision revision);

    StoredWorkflowSource insertWorkflowSource(int revId, WorkflowSource workflow)
        throws ResourceConflictException;

    StoredScheduleSource insertScheduleSource(int revId, ScheduleSource workflow)
        throws ResourceConflictException;

    void syncWorkflowsToRevision(int repoId, List<StoredWorkflowSource> sources)
        throws ResourceConflictException;

    void syncSchedulesToRevision(int repoId, List<Schedule> schedules)
        throws ResourceConflictException;

    //void deleteRepository(int repoId);  // TODO delete schedule
}
