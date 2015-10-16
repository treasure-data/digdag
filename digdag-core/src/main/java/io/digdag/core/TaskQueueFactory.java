package io.digdag.core;

public interface TaskQueueFactory
{
    String getType();

    TaskQueue getTaskQueue(int siteId, String name, ConfigSource config);
}
