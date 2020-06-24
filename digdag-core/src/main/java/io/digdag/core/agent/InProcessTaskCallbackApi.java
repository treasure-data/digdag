package io.digdag.core.agent;

import java.util.List;
import java.time.Instant;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.core.database.TransactionManager;
import io.digdag.spi.TaskResult;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ProjectStoreManager;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.workflow.AttemptRequest;
import io.digdag.core.workflow.AttemptBuilder;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.workflow.SessionAttemptConflictException;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.storage.ArchiveManager;
import io.digdag.core.queue.TaskQueueServerManager;
import io.digdag.core.log.LogServerManager;
import io.digdag.core.log.TaskLogger;
import io.digdag.spi.TaskQueueClient;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.LogFilePrefix;
import io.digdag.spi.StorageObject;
import io.digdag.spi.StorageFileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.util.Locale.ENGLISH;
import static io.digdag.core.log.LogServerManager.logFilePrefixFromSessionAttempt;

public class InProcessTaskCallbackApi
        implements TaskCallbackApi
{
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final ProjectStoreManager pm;
    private final SessionStoreManager sm;
    private final ArchiveManager archiveManager;
    private final LogServerManager lm;
    private final AttemptBuilder attemptBuilder;
    private final AgentId agentId;
    private final WorkflowExecutor exec;
    private final TransactionManager tm;
    private final TaskQueueClient queueClient;

    @Inject
    public InProcessTaskCallbackApi(
            ProjectStoreManager pm,
            SessionStoreManager sm,
            ArchiveManager archiveManager,
            TaskQueueServerManager qm,
            LogServerManager lm,
            AgentId agentId,
            AttemptBuilder attemptBuilder,
            WorkflowExecutor exec,
            TransactionManager tm)
    {
        this.pm = pm;
        this.sm = sm;
        this.archiveManager = archiveManager;
        this.lm = lm;
        this.agentId = agentId;
        this.attemptBuilder = attemptBuilder;
        this.exec = exec;
        this.tm = tm;
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
                    tm.begin(() -> sm.getSessionStore(request.getSiteId()) .getAttemptById(attemptId),
                            ResourceNotFoundException.class);
            prefix = logFilePrefixFromSessionAttempt(attempt);
        }
        catch (ResourceNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return lm.newInProcessTaskLogger(agentId, prefix, taskName);
    }

    @Override
    public void taskHeartbeat(int siteId,
            List<String> lockedIds, AgentId agentId, int lockSeconds)
    {
        tm.begin(() -> queueClient.taskHeartbeat(siteId, lockedIds, agentId.toString(), lockSeconds));
    }

    @Override
    public Optional<StorageObject> openArchive(TaskRequest request)
    {
        if (!request.getRevision().isPresent()) {
            return Optional.absent();
        }
        String revision = request.getRevision().get();

        try {
            return tm.<Optional<StorageObject>, ResourceNotFoundException, StorageFileNotFoundException>begin(() ->
                            archiveManager.openArchive(
                                    pm.getProjectStore(request.getSiteId()), request.getProjectId(), revision),
                    ResourceNotFoundException.class, StorageFileNotFoundException.class);
        }
        catch (ResourceNotFoundException ex) {
            throw new IllegalStateException(String.format(ENGLISH,
                        "Archive data for project id=%d revision='%s' is not found in database",
                        request.getProjectId(),
                        request.getRevision().or("")
                        ), ex);
        }
        catch (StorageFileNotFoundException ex) {
            throw new IllegalStateException(String.format(ENGLISH,
                        "Archive file for project id=%d revision='%s' is not found",
                        request.getProjectId(),
                        request.getRevision().or("")
                        ), ex);
        }
    }

    @Override
    public void taskSucceeded(int siteId,
            long taskId, String lockId, AgentId agentId,
            TaskResult result)
    {
        tm.begin(() -> exec.taskSucceeded(siteId, taskId, lockId, agentId, result));
    }

    @Override
    public void taskFailed(int siteId,
            long taskId, String lockId, AgentId agentId,
            Config error)
    {
        tm.begin(() -> exec.taskFailed(siteId, taskId, lockId, agentId, error));
    }

    @Override
    public void retryTask(int siteId,
            long taskId, String lockId, AgentId agentId,
            int retryInterval, Config retryStateParams,
            Optional<Config> error)
    {
        tm.begin(() -> exec.retryTask(siteId, taskId, lockId, agentId,
                retryInterval, retryStateParams, error));
    }

    @Override
    public StoredSessionAttempt startSession(
            int siteId,
            ProjectIdentifier projectIdentifier,
            String workflowName,
            Instant instant,
            Optional<String> retryAttemptName,
            Config overrideParams)
        throws ResourceNotFoundException, ResourceLimitExceededException, SessionAttemptConflictException
    {
        return tm.<StoredSessionAttempt, ResourceNotFoundException, ResourceLimitExceededException, SessionAttemptConflictException>begin(() -> {
                            ProjectStore projectStore = pm.getProjectStore(siteId);

                            //TODO check permission by AccessController for 'require' operator over another project.

                            StoredProject proj = projectIdentifier.byId() ?
                                projectStore.getProjectById(projectIdentifier.getId()) :
                                projectStore.getProjectByName(projectIdentifier.getName());

                            StoredWorkflowDefinitionWithProject def =
                                    projectStore.getLatestWorkflowDefinitionByName(proj.getId(), workflowName);

                            // use the HTTP request time as the runTime
                            AttemptRequest ar = attemptBuilder.buildFromStoredWorkflow(
                                    def,
                                    overrideParams,
                                    ScheduleTime.runNow(instant),
                                    retryAttemptName,
                                    Optional.absent(),
                                    ImmutableList.of(),
                                    Optional.absent());

                            // TODO FIXME SessionMonitor monitors is not set
                            StoredSessionAttemptWithSession attempt;
                            attempt = exec.submitWorkflow(siteId, ar, def);
                            return attempt;
                        },
                ResourceNotFoundException.class, ResourceLimitExceededException.class, SessionAttemptConflictException.class);
    }
}
