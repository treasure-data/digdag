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
        T call(RepositoryControl lockedRepository);
    }

    <T> T putRepository(Repository repository, RepositoryLockAction<T> func);


    List<StoredRevision> getRevisions(int repoId, int pageSize, Optional<Integer> lastId);

    StoredRevision getRevisionById(int revId)
        throws ResourceNotFoundException;

    StoredRevision getRevisionByName(int repoId, String revName)
        throws ResourceNotFoundException;

    StoredRevision getLatestActiveRevision(int repoId)
        throws ResourceNotFoundException;


    List<StoredWorkflowSource> getWorkflows(int revId, int pageSize, Optional<Integer> lastId);

    List<StoredWorkflowSourceWithRepository> getLatestActiveWorkflows(int pageSize, Optional<Integer> lastId);

    StoredWorkflowSource getWorkflowById(int wfId)
        throws ResourceNotFoundException;

    StoredWorkflowSource getWorkflowByName(int revId, String name)
        throws ResourceNotFoundException;
}
