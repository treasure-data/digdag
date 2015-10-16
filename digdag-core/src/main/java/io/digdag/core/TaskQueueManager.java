package io.digdag.core;

import java.util.Set;
import java.util.Map;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;

public class TaskQueueManager
{
    private final QueueDescStoreManager storeManager;
    private final ConfigSource defaultQueueConfig;
    private final Map<String, TaskQueueFactory> queueTypes;

    @Inject
    public TaskQueueManager(QueueDescStoreManager storeManager, ConfigSourceFactory cf, Set<TaskQueueFactory> factories)
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
        ConfigSource config = storeManager.getQueueDescStore(siteId)
            .getQueueDescByNameOrCreateDefault(name, defaultQueueConfig)
            .getConfig();
        return getTaskQueueFromConfigSource(siteId, name, config);
    }

    // used by executors
    public TaskQueue getTaskQueue(int siteId, String name)
    {
        ConfigSource config = storeManager.getQueueDescStore(siteId)
            .getQueueDescByName(name)
            .getConfig();
        return getTaskQueueFromConfigSource(siteId, name, config);
    }

    private TaskQueue getTaskQueueFromConfigSource(int siteId, String name, ConfigSource config)
    {
        String type = config.get("type", String.class);
        TaskQueueFactory factory = queueTypes.get(type);
        if (factory == null) {
            throw new ConfigException("Queue type " + type + " is not available");
        }
        return factory.getTaskQueue(siteId, name, config);
    }
}
