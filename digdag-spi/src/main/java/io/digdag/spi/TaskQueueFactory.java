package io.digdag.spi;

import io.digdag.client.config.Config;

public interface TaskQueueFactory
{
    String getType();

    TaskQueue getTaskQueue(int siteId, String name, Config config);
}
