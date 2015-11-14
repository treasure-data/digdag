package io.digdag.core.queue;

import java.util.Set;
import java.util.Map;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import io.digdag.core.spi.TaskQueue;
import io.digdag.core.spi.TaskQueueFactory;
import io.digdag.core.config.Config;
import io.digdag.core.config.ConfigException;
import io.digdag.core.config.ConfigFactory;
import io.digdag.core.repository.ResourceNotFoundException;

public class TaskQueueManager
{
    private final QueueDescStoreManager storeManager;
    private final Config defaultQueueConfig;
    private final Map<String, TaskQueueFactory> queueTypes;

    @Inject
    public TaskQueueManager(QueueDescStoreManager storeManager, ConfigFactory cf, Set<TaskQueueFactory> factories)
    {
        this.storeManager = storeManager;
        this.defaultQueueConfig = cf.create().set("type", "memory"); // TODO inject

        ImmutableMap.Builder<String, TaskQueueFactory> builder = ImmutableMap.builder();
        for (TaskQueueFactory factory : factories) {
            builder.put(factory.getType(), factory);
        }
        this.queueTypes = builder.build();
    }

    // used by agents
    public TaskQueue getOrCreateTaskQueue(int siteId, String name)
    {
        Config config = storeManager.getQueueDescStore(siteId)
            .getQueueDescByNameOrCreateDefault(name, defaultQueueConfig)
            .getConfig();
        return getTaskQueueFromConfig(siteId, name, config);
    }

    // used by executors
    public TaskQueue getTaskQueue(int siteId, String name)
        throws ResourceNotFoundException
    {
        Config config = storeManager.getQueueDescStore(siteId)
            .getQueueDescByName(name)
            .getConfig();
        return getTaskQueueFromConfig(siteId, name, config);
    }

    private TaskQueue getTaskQueueFromConfig(int siteId, String name, Config config)
    {
        String type = config.get("type", String.class);
        TaskQueueFactory factory = queueTypes.get(type);
        if (factory == null) {
            throw new ConfigException("Queue type " + type + " is not available");
        }
        return factory.getTaskQueue(siteId, name, config);
    }
}
