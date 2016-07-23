package io.digdag.core.queue;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskQueueServer;
import io.digdag.spi.TaskConflictException;
import io.digdag.spi.TaskNotFoundException;
import io.digdag.core.agent.AgentId;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.workflow.TaskQueueDispatcher;

public class QueueTaskQueueDispatcher
        implements TaskQueueDispatcher
{
    private static Logger logger = LoggerFactory.getLogger(TaskQueueDispatcher.class);

    private final QueueSettingStoreManager queueManager;
    private final TaskQueueServer taskQueueServer;

    @Inject
    public QueueTaskQueueDispatcher(
            QueueSettingStoreManager queueManager,
            TaskQueueServerManager queueServerManager)
    {
        this.queueManager = queueManager;
        this.taskQueueServer = queueServerManager.getTaskQueueServer();
    }

    @Override
    public void dispatch(TaskRequest request)
        throws ResourceNotFoundException, TaskConflictException
    {
        logger.trace("Dispatching request {}", request.getTaskName());
        logger.trace("  config: {}", request.getConfig());
        logger.trace("  stateParams: {}", request.getLastStateParams());

        if (request.getQueueName().isPresent()) {
            String queueName = request.getQueueName().get();
            int queueId = queueManager.getQueueIdByName(request.getSiteId(), queueName);
            taskQueueServer.enqueueQueueBoundTask(queueId, request);
        }
        else {
            taskQueueServer.enqueueDefaultQueueTask(request);
        }
    }

    @Override
    public void taskFinished(int siteId, String lockId, AgentId agentId)
        throws TaskConflictException, TaskNotFoundException
    {
        taskQueueServer.deleteTask(siteId, lockId, agentId.toString());
    }
}
