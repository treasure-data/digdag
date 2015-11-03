package io.digdag.core;

import io.digdag.core.config.Config;

public interface TaskQueueFactory
{
    String getType();

    TaskQueue getTaskQueue(int siteId, String name, Config config);
}
