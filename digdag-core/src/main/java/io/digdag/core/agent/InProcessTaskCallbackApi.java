package io.digdag.core.agent;

import java.util.List;
import java.time.Instant;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskResult;
import io.digdag.core.session.Session;
import io.digdag.core.repository.StoredRepository;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.repository.RepositoryStore;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.workflow.AttemptRequest;
import io.digdag.core.workflow.AttemptBuilder;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.SessionStateFlags;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.queue.TaskQueueManager;
import io.digdag.core.log.LogServerManager;
import io.digdag.core.log.TaskLogger;
import io.digdag.spi.TaskQueueClient;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.LogFilePrefix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static io.digdag.core.log.LogServerManager.logFilePrefixFromSessionAttempt;

public class InProcessTaskCallbackApi
        implements TaskCallbackApi
{
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final RepositoryStoreManager rm;
    private final SessionStoreManager sm;
    private final LogServerManager lm;
    private final AttemptBuilder attemptBuilder;
    private final WorkflowExecutor exec;
    private final TaskQueueClient queueClient;

    @Inject
    public InProcessTaskCallbackApi(
            RepositoryStoreManager rm,
            SessionStoreManager sm,
            TaskQueueManager qm,
            LogServerManager lm,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor exec)
    {
        this.rm = rm;
        this.sm = sm;
        this.lm = lm;
        this.attemptBuilder = attemptBuilder;
        this.exec = exec;
        this.queueClient = qm.getInProcessTaskQueueClient();
    }

    @Override
    public TaskLogger newTaskLogger(TaskRequest request)
    {
        long attemptId = request.getAttemptId();
        String taskName = request.getTaskName();
        LogFilePrefix prefix;
        try {
            StoredSessionAttemptWithSession attempt =
                sm.getSessionStore(request.getSiteId())
                .getSessionAttemptById(attemptId);
            prefix = logFilePrefixFromSessionAttempt(attempt);
        }
        catch (ResourceNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return lm.newInProcessTaskLogger(prefix, taskName);
    }

    @Override
    public void taskHeartbeat(int siteId,
            List<String> lockedIds, AgentId agentId, int lockSeconds)
    {
        queueClient.taskHeartbeat(siteId, lockedIds, agentId.toString(), lockSeconds);
    }

    @Override
    public void taskSucceeded(int siteId,
            long taskId, String lockId, AgentId agentId,
            TaskResult result)
    {
        exec.taskSucceeded(siteId, taskId, lockId, agentId,
                result);
    }

    @Override
    public void taskFailed(int siteId,
            long taskId, String lockId, AgentId agentId,
            Config error)
    {
        exec.taskFailed(siteId, taskId, lockId, agentId,
                error);
    }

    @Override
    public void retryTask(int siteId,
            long taskId, String lockId, AgentId agentId,
            int retryInterval, Config retryStateParams,
            Optional<Config> error)
    {
        exec.retryTask(siteId, taskId, lockId, agentId,
                retryInterval, retryStateParams, error);
    }

    @Override
    public SessionStateFlags startSession(
            int siteId,
            int repositoryId,
            String workflowName,
            Instant instant,
            Optional<String> retryAttemptName,
            Config overwriteParams)
        throws ResourceNotFoundException
    {
        RepositoryStore repoStore = rm.getRepositoryStore(siteId);

        StoredRepository repo = repoStore.getRepositoryById(repositoryId);
        StoredWorkflowDefinitionWithRepository def = repoStore.getLatestWorkflowDefinitionByName(repo.getId(), workflowName);

        // use the HTTP request time as the runTime
        AttemptRequest ar = attemptBuilder.buildFromStoredWorkflow(
                def,
                overwriteParams,
                ScheduleTime.runNow(instant),
                retryAttemptName);

        // TODO FIXME SessionMonitor monitors is not set
        try {
            StoredSessionAttemptWithSession attempt = exec.submitWorkflow(siteId, ar, def);
            return attempt.getStateFlags();
        }
        catch (SessionAttemptConflictException ex) {
            return ex.getConflictedSession().getStateFlags();
        }
    }
}
