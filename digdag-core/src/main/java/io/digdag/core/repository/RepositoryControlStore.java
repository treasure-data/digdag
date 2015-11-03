package io.digdag.core.repository;

import java.util.List;
import com.google.common.base.Optional;

public interface RepositoryControlStore
{
    List<StoredWorkflowSource> getWorkflows(int revId, int pageSize, Optional<Integer> lastId);

    StoredRevision putRevision(int repoId, Revision revision);

    StoredWorkflowSource putWorkflow(int revId, WorkflowSource workflow);
}
