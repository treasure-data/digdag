package io.digdag.core.agent;

import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskReport;
import io.digdag.core.session.Session;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredWorkflowSourceWithRepository;
import io.digdag.core.repository.RepositoryStore;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.SessionRelation;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.TaskStateCode;

public class LocalInProcessTaskCallbackApi
        implements TaskCallbackApi
{
    private final int siteId;
    private final RepositoryStoreManager rm;
    private final SessionStoreManager sm;
    private final WorkflowExecutor exec;

    @Inject
    public LocalInProcessTaskCallbackApi(
            RepositoryStoreManager rm,
            SessionStoreManager sm,
            WorkflowExecutor exec)
    {
        this.siteId = 0;
        this.rm = rm;
        this.sm = sm;
        this.exec = exec;
    }

    @Override
    public void taskSucceeded(long taskId,
            Config stateParams, Config subtaskConfig,
            TaskReport report)
    {
        exec.taskSucceeded(taskId, stateParams, subtaskConfig, report);
    }

    @Override
    public void taskFailed(long taskId,
            Config error, Config stateParams,
            Optional<Integer> retryInterval)
    {
        exec.taskFailed(taskId, error, stateParams, retryInterval);
    }

    @Override
    public void taskPollNext(long taskId,
            Config stateParams, int retryInterval)
    {
        exec.taskPollNext(taskId, stateParams, retryInterval);
    }

    @Override
    public TaskStateCode startSession(String repositoryName,
            String workflowName, Session session)
    {
        RepositoryStore repoStore = rm.getRepositoryStore(siteId);
        SessionStore sessionStore = sm.getSessionStore(siteId);

        StoredWorkflowSourceWithRepository wf;
        try {
            StoredRepository repo = repoStore.getRepositoryByName(repositoryName);
            wf = repoStore.getLatestActiveWorkflowSourceByName(repo.getId(), workflowName);
        }
        catch (ResourceNotFoundException ex) {
            throw new RuntimeException(ex);
        }

        StoredSession stored;
        try {
            stored = sessionStore.getSessionByName(session.getName());
        }
        catch (ResourceNotFoundException try1) {
            try {
                stored = exec.submitWorkflow(siteId,
                        wf, Optional.absent(),
                        session, Optional.of(SessionRelation.ofWorkflow(wf.getRepository().getId(), wf.getRevisionId(), wf.getId())),
                        ImmutableList.of());
            }
            catch (ResourceConflictException try2) {
                try {
                    stored = sessionStore.getSessionByName(session.getName());
                }
                catch (ResourceNotFoundException asyncDeleted) {
                    throw new RuntimeException(asyncDeleted);
                }
            }
            catch  (TaskMatchPattern.NoMatchException | TaskMatchPattern.MultipleTaskMatchException neverHappens) {
                throw new RuntimeException(neverHappens);
            }
        }

        try {
            return sessionStore.getRootState(stored.getId());
        }
        catch (ResourceNotFoundException asyncDeleted) {
            throw new RuntimeException(asyncDeleted);
        }
    }
}
