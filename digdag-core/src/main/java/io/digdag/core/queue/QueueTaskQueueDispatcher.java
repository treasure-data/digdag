package io.digdag.core.queue;

import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskQueueRequest;
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
    public void dispatch(int siteId, TaskQueueRequest request)
        throws ResourceNotFoundException, TaskConflictException
    {
        if (request.getQueueName().isPresent()) {
            int queueId = queueManager.getQueueIdByName(siteId, request.getQueueName().get());
            taskQueueServer.enqueueQueueBoundTask(queueId, request);
        }
        else {
            taskQueueServer.enqueueDefaultQueueTask(siteId, request);
        }
    }

    @Override
    public void taskFinished(int siteId, String lockId, AgentId agentId)
        throws TaskConflictException, TaskNotFoundException
    {
        taskQueueServer.deleteTask(siteId, lockId, agentId.toString());
    }

    @Override
    public boolean deleteInconsistentTask(String lockId)
    {
        return taskQueueServer.forceDeleteTask(lockId);
    }
}
