package io.digdag.core.repository;

import com.google.common.base.Optional;
import io.digdag.spi.ac.AccessController;

import java.util.List;

public interface ProjectStore
{
    List<StoredProject> getProjects(int pageSize, Optional<Integer> lastId, Optional<String> namePattern, AccessController.ListFilter acFilter);

    List<StoredProjectWithRevision> getProjectsWithLatestRevision(int pageSize, Optional<Integer> lastId, Optional<String> namePattern, AccessController.ListFilter acFilter);

    ProjectMap getProjectsByIdList(List<Integer> projIdList);

    StoredProject getProjectById(int projId)
        throws ResourceNotFoundException;

    StoredProject getProjectByName(String projName)
        throws ResourceNotFoundException;

    interface ProjectLockAction <T>
    {
        T call(ProjectControlStore store, StoredProject storedProject)
            throws ResourceConflictException;
    }

    <T> T putAndLockProject(Project project, ProjectLockAction<T> func)
        throws ResourceConflictException;

    interface ProjectObsoleteAction <T>
    {
        T call(ProjectControlStore store, StoredProject storedProject)
            throws ResourceNotFoundException;
    }

    <T> T deleteProject(int projId, ProjectObsoleteAction<T> func)
        throws ResourceNotFoundException;


    StoredRevision getRevisionById(int revId)
        throws ResourceNotFoundException;

    StoredRevision getRevisionByName(int projId, String revName)
        throws ResourceNotFoundException;

    StoredRevision getLatestRevision(int projId)
        throws ResourceNotFoundException;

    List<StoredRevision> getRevisions(int projId, int pageSize, Optional<Integer> lastId);

    byte[] getRevisionArchiveData(int revId)
            throws ResourceNotFoundException;


    List<StoredWorkflowDefinition> getWorkflowDefinitions(int revId, int pageSize, Optional<Long> lastId, AccessController.ListFilter acFilter);

    StoredWorkflowDefinition getWorkflowDefinitionByName(int revId, String name)
            throws ResourceNotFoundException;

    StoredWorkflowDefinitionWithProject getWorkflowDefinitionById(long wfId)
            throws ResourceNotFoundException;

    StoredWorkflowDefinitionWithProject getLatestWorkflowDefinitionByName(int projId, String name)
            throws ResourceNotFoundException;

    // For backward compatibility
    default List<StoredWorkflowDefinitionWithProject> getLatestActiveWorkflowDefinitions(
            int pageSize, Optional<Long> lastId, Optional<String> namePattern, AccessController.ListFilter acFilter)
            throws ResourceNotFoundException
    {
        // ascending is true and searchProjectNmae is disabled.
        return getLatestActiveWorkflowDefinitions(pageSize, lastId, true, namePattern, false, acFilter);
    }

    List<StoredWorkflowDefinitionWithProject> getLatestActiveWorkflowDefinitions(
            int pageSize, Optional<Long> lastId, boolean ascending, Optional<String> namePattern,
            boolean searchProjectName, AccessController.ListFilter acFilter)
            throws ResourceNotFoundException;

    TimeZoneMap getWorkflowTimeZonesByIdList(List<Long> defIdList);
}
