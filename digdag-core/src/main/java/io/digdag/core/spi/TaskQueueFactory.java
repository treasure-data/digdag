package io.digdag.core.spi;

import io.digdag.core.config.Config;
import io.digdag.core.spi.TaskQueue;

public interface TaskQueueFactory
{
    String getType();

    TaskQueue getTaskQueue(int siteId, String name, Config config);
}
