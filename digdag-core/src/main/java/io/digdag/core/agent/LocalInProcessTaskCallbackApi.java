package io.digdag.core.agent;

import java.time.Instant;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskReport;
import io.digdag.core.session.Session;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.repository.RepositoryStore;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.workflow.AttemptRequest;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.SessionStateFlags;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredSessionAttemptWithSession;

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
    public SessionStateFlags startSession(
            int repositoryId,
            String workflowName,
            Instant instant,
            Optional<String> retryAttemptName,
            ZoneId defaultTimeZone,
            Config overwriteParams)
    {
        RepositoryStore repoStore = rm.getRepositoryStore(siteId);
        SessionStore sessionStore = sm.getSessionStore(siteId);

        StoredWorkflowDefinitionWithRepository def;
        try {
            StoredRepository repo = repoStore.getRepositoryById(repositoryId);
            def = repoStore.getLatestWorkflowDefinitionByName(repo.getId(), workflowName);
        }
        catch (ResourceNotFoundException ex) {
            throw new RuntimeException(ex);
        }

        AttemptRequest ar = AttemptRequest.builder()
            .repositoryId(def.getRepository().getId())
            .workflowName(def.getName())
            .instant(instant)
            .retryAttemptName(retryAttemptName)
            .defaultTimeZone(defaultTimeZone)
            .defaultParams(def.getRevisionDefaultParams())
            .overwriteParams(overwriteParams)
            .storedWorkflowDefinitionId(Optional.of(def.getId()))
            .build();

        // TODO FIXME SessionMonitor monitors is not set
        StoredSessionAttemptWithSession attempt = exec.submitWorkflow(siteId, ar, def, ImmutableList.of());

        return attempt.getStateFlags();
    }
}
