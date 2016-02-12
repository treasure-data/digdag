package io.digdag.core.workflow;

import com.google.inject.Inject;
import io.digdag.spi.TaskRequest;
import io.digdag.core.queue.TaskQueueManager;
import io.digdag.spi.TaskQueue;
import io.digdag.core.repository.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.spi.TaskQueueServer;
import io.digdag.spi.TaskStateException;
import io.digdag.core.agent.AgentId;

public class TaskQueueDispatcher
{
    private static Logger logger = LoggerFactory.getLogger(TaskQueueDispatcher.class);

    private final TaskQueueManager manager;

    @Inject
    public TaskQueueDispatcher(TaskQueueManager manager)
    {
        this.manager = manager;
    }

    public void dispatch(TaskRequest request)
        throws ResourceConflictException
    {
        logger.trace("Dispatching request {}", request.getTaskName());
        logger.trace("  config: {}", request.getConfig());
        logger.trace("  stateParams: {}", request.getLastStateParams());

        TaskQueueServer queue = manager.getTaskQueueServer(request.getSiteId());
        try {
            queue.enqueue(request);
        }
        catch (TaskStateException ex) {
            if (ex.getCause() instanceof ResourceConflictException) {
                throw (ResourceConflictException) ex.getCause();
            }
            throw new ResourceConflictException(ex);
        }
    }

    public void taskHeartbeat(int siteId, String lockId, AgentId agentId)
    {
        TaskQueueServer queue = manager.getTaskQueueServer(siteId);
        queue.taskHeartbeat(siteId, lockId, agentId.toString());
    }

    public void taskFinished(int siteId, String lockId, AgentId agentId)
    {
        TaskQueueServer queue = manager.getTaskQueueServer(siteId);
        queue.delete(siteId, lockId, agentId.toString());
    }
}
