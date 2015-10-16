package io.digdag.core;

import com.google.inject.Inject;

public class TaskQueueDispatcher
{
    private final TaskQueueManager manager;

    @Inject
    public TaskQueueDispatcher(TaskQueueManager manager)
    {
        this.manager = manager;
    }

    public void dispatch(Action action)
    {
        System.out.println("Running task: "+action.getFullName());
        System.out.println("  params: "+action.getParams());
        System.out.println("  stateParams: "+action.getStateParams());

        String queueName = action.getConfig().get("queue", String.class, "local");  // TODO configurable default queue name
        TaskQueue queue = manager.getTaskQueue(action.getSiteId(), queueName);
        queue.put(action);
    }
}
