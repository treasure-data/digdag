package io.digdag.core;

public interface RepositoryStore
        extends Store
{
    Pageable<StoredRepository> getRepositories(int pageSize);

    StoredRepository getRepositoryById(int repoId);

    StoredRepository getRepositoryByName(String name);

    StoredRepository putRepository(Repository repository);

    void deleteRepository(int repoId);


    Pageable<StoredRevision> getRevisions(int repoId, int pageSize);

    StoredRevision getRevisionById(int revId);

    StoredRevision getRevisionByName(int repoId, String name);

    StoredRevision getLatestActiveRevision(int repoId);

    StoredRevision putRevision(int repoId, Revision revision);

    void deleteRevision(int revId);


    Pageable<StoredWorkflow> getWorkflows(int revId, int pageSize);

    Pageable<StoredWorkflowWithRepository> getAllLatestActiveWorkflows();

    StoredWorkflow getWorkflowById(int wfId);

    StoredWorkflow getWorkflowByName(int revId, String name);

    StoredWorkflow putWorkflow(int revId, Workflow workflow);

    void deleteWorkflow(int wfId);
}
