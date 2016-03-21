package io.digdag.core.repository;

import java.util.List;
import com.google.common.base.Optional;

public interface RepositoryStore
{
    List<StoredRepository> getRepositories(int pageSize, Optional<Integer> lastId);

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


    List<StoredRevision> getRevisions(int repoId, int pageSize, Optional<Integer> lastId);

    List<StoredRevision> getLatestRevisions(int pageSize, Optional<Integer> lastId);

    StoredRevision getRevisionById(int revId)
        throws ResourceNotFoundException;

    StoredRevision getRevisionByName(int repoId, String revName)
        throws ResourceNotFoundException;

    StoredRevision getLatestRevision(int repoId)
        throws ResourceNotFoundException;

    byte[] getRevisionArchiveData(int revId)
            throws ResourceNotFoundException;


    List<StoredWorkflowDefinition> getWorkflowDefinitions(int revId, int pageSize, Optional<Long> lastId);

    StoredWorkflowDefinition getWorkflowDefinitionById(long wfId)
        throws ResourceNotFoundException;

    StoredWorkflowDefinition getWorkflowDefinitionByName(int revId, String name)
        throws ResourceNotFoundException;

    StoredWorkflowDefinitionWithRepository getLatestWorkflowDefinitionByName(int repoId, String name)
        throws ResourceNotFoundException;
}
