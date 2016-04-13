package io.digdag.core.repository;

public interface ProjectStoreManager
{
    ProjectStore getProjectStore(int siteId);

    // used by the standard ScheduleHandler
    StoredWorkflowDefinitionWithProject getWorkflowDetailsById(long wfId)
        throws ResourceNotFoundException;

    StoredProject getProjectByIdInternal(int projId)
        throws ResourceNotFoundException;

    StoredRevision getRevisionOfWorkflowDefinition(long wfId)
        throws ResourceNotFoundException;
}
