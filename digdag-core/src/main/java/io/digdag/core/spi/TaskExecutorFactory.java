package io.digdag.core.spi;

import io.digdag.core.config.Config;

public interface TaskExecutorFactory
{
    String getType();

    TaskExecutor newTaskExecutor(Config config, Config params, Config state);
}
