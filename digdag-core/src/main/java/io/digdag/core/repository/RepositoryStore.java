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


    List<StoredWorkflowSource> getWorkflowSources(int revId, int pageSize, Optional<Integer> lastId);

    List<StoredWorkflowSourceWithRepository> getLatestActiveWorkflowSources(int pageSize, Optional<Integer> lastId);

    StoredWorkflowSource getWorkflowSourceById(int wfId)
        throws ResourceNotFoundException;

    StoredWorkflowSource getWorkflowSourceByName(int revId, String name)
        throws ResourceNotFoundException;


    //List<StoredScheduleSource> getScheduleSources(int revId, int pageSize, Optional<Integer> lastId);

    //List<StoredScheduleSourceWithRepository> getLatestActiveScheduleSources(int pageSize, Optional<Integer> lastId);

    //StoredScheduleSource getScheduleSourceById(int wfId)
    //    throws ResourceNotFoundException;

    //StoredScheduleSource getScheduleSourceByName(int revId, String name)
    //    throws ResourceNotFoundException;
}
