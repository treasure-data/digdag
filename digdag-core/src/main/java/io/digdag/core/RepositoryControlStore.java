package io.digdag.core;

public interface RepositoryControlStore
{
    StoredRevision putRevision(int repoId, Revision revision);

    StoredWorkflowSource putWorkflow(int revId, WorkflowSource workflow);
}
