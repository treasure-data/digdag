package io.digdag.core.queue;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskQueueServer;
import io.digdag.spi.TaskStateException;
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
        throws ResourceNotFoundException, ResourceConflictException
    {
        logger.trace("Dispatching request {}", request.getTaskName());
        logger.trace("  config: {}", request.getConfig());
        logger.trace("  stateParams: {}", request.getLastStateParams());

        try {
            if (request.getQueueName().isPresent()) {
                String queueName = request.getQueueName().get();
                int queueId = queueManager.getQueueIdByName(request.getSiteId(), queueName);
                taskQueueServer.enqueueQueueBoundTask(queueId, request);
            }
            else {
                taskQueueServer.enqueueDefaultQueueTask(request);
            }
        }
        catch (TaskStateException ex) {
            if (ex.getCause() instanceof ResourceConflictException) {
                throw (ResourceConflictException) ex.getCause();
            }
            throw new ResourceConflictException(ex);
        }
    }

    @Override
    public void taskFinished(int siteId, String lockId, AgentId agentId)
    {
        taskQueueServer.deleteTask(siteId, lockId, agentId.toString());
    }
}
