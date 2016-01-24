package io.digdag.core.repository;

public interface RepositoryStoreManager
{
    RepositoryStore getRepositoryStore(int siteId);

    // used by the standard ScheduleHandler
    StoredWorkflowDefinitionWithRepository getWorkflowDetailsById(long wfId)
        throws ResourceNotFoundException;

    StoredRepository getRepositoryByIdInternal(int repoId)
        throws ResourceNotFoundException;
}
