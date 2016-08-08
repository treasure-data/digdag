package io.digdag.core.queue;

import java.util.Set;
import java.util.Map;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableMap;
import io.digdag.spi.TaskQueueServer;
import io.digdag.spi.TaskQueueClient;
import io.digdag.spi.TaskQueueFactory;
import io.digdag.client.config.Config;

public class TaskQueueServerManager
{
    private final TaskQueueServer taskQueueServer;

    @Inject
    public TaskQueueServerManager(Config systemConfig, Set<TaskQueueFactory> factories)
    {
        ImmutableMap.Builder<String, TaskQueueFactory> builder = ImmutableMap.builder();
        for (TaskQueueFactory factory : factories) {
            builder.put(factory.getType(), factory);
        }
        Map<String, TaskQueueFactory> queueTypes = builder.build();
        String type = systemConfig.get("queue-server.type", String.class, "database");
        TaskQueueFactory factory = queueTypes.get(type);
        this.taskQueueServer = factory.newServer(systemConfig);
    }

    public TaskQueueServer getTaskQueueServer()
    {
        return taskQueueServer;
    }

    // used by LocalAgentManager, InProcessTaskCallbackApi digdag-server (TaskResource, which is not implemented yet)
    public TaskQueueClient getInProcessTaskQueueClient()
    {
        return getTaskQueueServer();
    }
}
