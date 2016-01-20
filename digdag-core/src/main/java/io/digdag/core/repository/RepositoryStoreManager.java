package io.digdag.core.repository;

public interface RepositoryStoreManager
{
    RepositoryStore getRepositoryStore(int siteId);

    // used by the standard ScheduleHandler
    StoredWorkflowSourceWithRepository getWorkflowDetailsById(int wfId)
        throws ResourceNotFoundException;
}
