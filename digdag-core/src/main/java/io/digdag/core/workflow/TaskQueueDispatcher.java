package io.digdag.core.workflow;

import com.google.inject.Inject;
import io.digdag.core.queue.Action;
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

    public void dispatch(Action action)
            throws ResourceNotFoundException
    {
        logger.trace("Dispatching action {}", action.getFullName());
        logger.trace("  params: {}", action.getParams());
        logger.trace("  stateParams: {}", action.getStateParams());

        String queueName = action.getConfig().get("queue", String.class, "local");  // TODO configurable default queue name
        TaskQueue queue = manager.getTaskQueue(action.getSiteId(), queueName);
        queue.put(action);
    }
}
