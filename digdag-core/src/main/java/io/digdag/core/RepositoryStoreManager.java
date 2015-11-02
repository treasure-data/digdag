package io.digdag.core;

public interface RepositoryStoreManager
{
    RepositoryStore getRepositoryStore(int siteId);

    // used by StandardScheduleStarter & SlaExecutor
    StoredWorkflowSourceWithRepository getWorkflowDetailsById(int wfId);
}
