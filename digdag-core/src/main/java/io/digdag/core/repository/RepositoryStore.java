package io.digdag.core.repository;

import java.util.List;
import java.util.Map;
import java.time.ZoneId;
import com.google.common.base.Optional;

public interface RepositoryStore
{
    List<StoredRepository> getRepositories(int pageSize, Optional<Integer> lastId);

    RepositoryMap getRepositoriesByIdList(List<Integer> repoIdList);

    StoredRepository getRepositoryById(int repoId)
        throws ResourceNotFoundException;

    StoredRepository getRepositoryByName(String repoName)
        throws ResourceNotFoundException;

    interface RepositoryLockAction <T>
    {
        T call(RepositoryControlStore store, StoredRepository storedRepo)
            throws ResourceConflictException;
    }

    <T> T putAndLockRepository(Repository repository, RepositoryLockAction<T> func)
        throws ResourceConflictException;


    StoredRevision getRevisionById(int revId)
        throws ResourceNotFoundException;

    StoredRevision getRevisionByName(int repoId, String revName)
        throws ResourceNotFoundException;

    StoredRevision getLatestRevision(int repoId)
        throws ResourceNotFoundException;

    List<StoredRevision> getRevisions(int repoId, int pageSize, Optional<Integer> lastId);

    byte[] getRevisionArchiveData(int revId)
            throws ResourceNotFoundException;


    List<StoredWorkflowDefinition> getWorkflowDefinitions(int revId, int pageSize, Optional<Long> lastId);

    StoredWorkflowDefinition getWorkflowDefinitionByName(int revId, PackageName packageName, String name)
        throws ResourceNotFoundException;

    StoredWorkflowDefinitionWithRepository getWorkflowDefinitionById(long wfId)
        throws ResourceNotFoundException;

    StoredWorkflowDefinitionWithRepository getLatestWorkflowDefinitionByName(int repoId, PackageName packageName, String name)
        throws ResourceNotFoundException;

    TimeZoneMap getWorkflowTimeZonesByIdList(List<Long> defIdList);
}
