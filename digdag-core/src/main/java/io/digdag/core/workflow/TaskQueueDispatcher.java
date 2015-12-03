package io.digdag.core.workflow;

import com.google.inject.Inject;
import io.digdag.core.spi.TaskRequest;
import io.digdag.core.queue.TaskQueueManager;
import io.digdag.core.spi.TaskQueue;
import io.digdag.core.repository.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            throws ResourceNotFoundException
    {
        logger.trace("Dispatching request {}", request.getTaskInfo().getFullName());
        logger.trace("  config: {}", request.getConfig());
        logger.trace("  stateParams: {}", request.getLastStateParams());

        String queueName = request.getConfig().get("queue", String.class, "local");  // TODO configurable default queue name
        TaskQueue queue = manager.getTaskQueue(request.getTaskInfo().getSiteId(), queueName);
        queue.put(request);
    }
}
