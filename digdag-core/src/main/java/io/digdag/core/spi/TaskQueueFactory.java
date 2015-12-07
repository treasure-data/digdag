package io.digdag.core.spi;

import io.digdag.core.spi.config.Config;

public interface TaskQueueFactory
{
    String getType();

    TaskQueue getTaskQueue(int siteId, String name, Config config);
}
