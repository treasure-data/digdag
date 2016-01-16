package io.digdag.core.repository;

import java.util.List;
import com.google.common.base.Optional;

public interface RepositoryControlStore
{
    List<StoredWorkflowSource> getWorkflowSources(int revId, int pageSize, Optional<Integer> lastId);

    StoredRevision putRevision(int repoId, Revision revision);

    StoredWorkflowSource insertWorkflowSource(int revId, WorkflowSource workflow)
        throws ResourceConflictException;

    StoredScheduleSource insertScheduleSource(int revId, ScheduleSource workflow)
        throws ResourceConflictException;

    //void deleteRepository(int repoId);  // TODO delete schedule
}
