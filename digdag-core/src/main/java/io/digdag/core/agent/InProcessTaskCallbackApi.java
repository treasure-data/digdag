package io.digdag.core.agent;

import java.time.Instant;
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
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.workflow.AttemptRequest;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.SessionStateFlags;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InProcessTaskCallbackApi
        implements TaskCallbackApi
{
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final int localSiteId;
    private final AgentId localAgentId;
    private final RepositoryStoreManager rm;
    private final SessionStoreManager sm;
    private final WorkflowExecutor exec;

    @Inject
    public InProcessTaskCallbackApi(
            AgentId localAgentId,
            RepositoryStoreManager rm,
            SessionStoreManager sm,
            WorkflowExecutor exec)
    {
        this.localSiteId = 0;
        this.localAgentId = localAgentId;
        this.rm = rm;
        this.sm = sm;
        this.exec = exec;
    }

    @Override
    public void taskHeartbeat(String lockId, AgentId agentId)
    {
        exec.taskHeartbeat(localSiteId, lockId, agentId);
    }

    @Override
    public void taskSucceeded(long taskId, String lockId, AgentId agentId,
            Config stateParams, Config subtaskConfig,
            TaskReport report)
    {
        exec.taskSucceeded(localSiteId, taskId, lockId, agentId,
                stateParams, subtaskConfig, report);
    }

    @Override
    public void taskFailed(long taskId, String lockId, AgentId agentId,
            Config error, Config stateParams,
            Optional<Integer> retryInterval)
    {
        exec.taskFailed(localSiteId, taskId, lockId, agentId,
                error, stateParams, retryInterval);
    }

    @Override
    public void taskPollNext(long taskId, String lockId, AgentId agentId,
            Config stateParams, int retryInterval)
    {
        exec.taskPollNext(localSiteId, taskId, lockId, agentId,
                stateParams, retryInterval);
    }

    @Override
    public SessionStateFlags startSession(
            int repositoryId,
            String workflowName,
            Instant instant,
            Optional<String> retryAttemptName,
            Config overwriteParams)
    {
        RepositoryStore repoStore = rm.getRepositoryStore(localSiteId);
        SessionStore sessionStore = sm.getSessionStore(localSiteId);

        StoredWorkflowDefinitionWithRepository def;
        try {
            StoredRepository repo = repoStore.getRepositoryById(repositoryId);
            def = repoStore.getLatestWorkflowDefinitionByName(repo.getId(), workflowName);
        }
        catch (ResourceNotFoundException ex) {
            throw new RuntimeException(ex);
        }

        AttemptRequest ar = AttemptRequest.builder()
            .stored(AttemptRequest.Stored.of(def))
            .workflowName(def.getName())
            .instant(instant)
            .retryAttemptName(retryAttemptName)
            .overwriteParams(overwriteParams)
            .build();

        // TODO FIXME SessionMonitor monitors is not set
        try {
            StoredSessionAttemptWithSession attempt = exec.submitWorkflow(localSiteId, ar, def, ImmutableList.of());
            return attempt.getStateFlags();
        }
        catch (SessionAttemptConflictException ex) {
            return ex.getConflictedSession().getStateFlags();
        }
    }
}
