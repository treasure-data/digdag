package io.digdag.core.repository;

public interface RepositoryStoreManager
{
    RepositoryStore getRepositoryStore(int siteId);

    // used by StandardScheduleStarter
    StoredWorkflowSourceWithRepository getWorkflowDetailsById(int wfId);
}
