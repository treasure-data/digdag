package io.digdag.core;

import java.util.List;
import com.google.common.base.Optional;

public interface RepositoryStore
        extends Store
{
    List<StoredRepository> getAllRepositories();  // TODO only for testing

    List<StoredRepository> getRepositories(int pageSize, Optional<Integer> lastId);

    StoredRepository getRepositoryById(int repoId);

    StoredRepository getRepositoryByName(String name);

    StoredRepository putRepository(Repository repository);

    void deleteRepository(int repoId);


    List<StoredRevision> getAllRevisions(int repoId);  // TODO only for testing

    List<StoredRevision> getRevisions(int repoId, int pageSize, Optional<Integer> lastId);

    StoredRevision getRevisionById(int revId);

    StoredRevision getRevisionByName(int repoId, String name);

    StoredRevision getLatestActiveRevision(int repoId);

    StoredRevision putRevision(int repoId, Revision revision);

    void deleteRevision(int revId);


    List<StoredWorkflowSource> getAllWorkflows(int revId);  // TODO only for testing

    List<StoredWorkflowSource> getWorkflows(int revId, int pageSize, Optional<Integer> lastId);

    List<StoredWorkflowSourceWithRepository> getAllLatestActiveWorkflows();

    StoredWorkflowSource getWorkflowById(int wfId);

    StoredWorkflowSource getWorkflowByName(int revId, String name);

    StoredWorkflowSource putWorkflow(int revId, WorkflowSource workflow);

    void deleteWorkflow(int wfId);
}
