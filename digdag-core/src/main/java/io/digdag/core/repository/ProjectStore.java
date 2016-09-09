package io.digdag.core.repository;

import java.util.List;
import java.util.Map;
import java.time.ZoneId;
import com.google.common.base.Optional;

public interface ProjectStore
{
    List<StoredProject> getProjects(int pageSize, Optional<Integer> lastId);

    ProjectMap getProjectsByIdList(List<Integer> projIdList);

    StoredProject getProjectById(int projId)
        throws ResourceNotFoundException;

    StoredProject getProjectByName(String projName)
        throws ResourceNotFoundException;

    interface ProjectLockAction <T>
    {
        T call(ProjectControlStore store, StoredProject storedProject)
            throws ResourceNotFoundException, ResourceConflictException;
    }

    <T> T lockProjectById(int projId, ProjectLockAction<T> func)
        throws ResourceNotFoundException, ResourceConflictException;

    interface NewProjectLockAction <T>
    {
        T call(ProjectControlStore store, StoredProject storedProject)
            throws ResourceConflictException;
    }

    <T> T putAndLockProject(Project project, NewProjectLockAction<T> func)
        throws ResourceConflictException;

    interface ProjectObsoleteAction <T>
    {
        T call(ProjectControlStore store, StoredProject storedProject)
            throws ResourceNotFoundException;
    }

    <T> T deleteProject(int projId, ProjectObsoleteAction<T> func)
        throws ResourceNotFoundException;


    List<String> listSecrets(int projId, String scope);

    Optional<String> getSecretIfExists(int projId, String scope, String key);


    StoredRevision getRevisionById(int revId)
        throws ResourceNotFoundException;

    StoredRevision getRevisionByName(int projId, String revName)
        throws ResourceNotFoundException;

    StoredRevision getLatestRevision(int projId)
        throws ResourceNotFoundException;

    List<StoredRevision> getRevisions(int projId, int pageSize, Optional<Integer> lastId);

    byte[] getRevisionArchiveData(int revId)
            throws ResourceNotFoundException;


    List<StoredWorkflowDefinition> getWorkflowDefinitions(int revId, int pageSize, Optional<Long> lastId);

    StoredWorkflowDefinition getWorkflowDefinitionByName(int revId, String name)
        throws ResourceNotFoundException;

    StoredWorkflowDefinitionWithProject getWorkflowDefinitionById(long wfId)
        throws ResourceNotFoundException;

    StoredWorkflowDefinitionWithProject getLatestWorkflowDefinitionByName(int projId, String name)
        throws ResourceNotFoundException;

    TimeZoneMap getWorkflowTimeZonesByIdList(List<Long> defIdList);
}
