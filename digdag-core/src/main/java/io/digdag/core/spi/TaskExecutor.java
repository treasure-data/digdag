package io.digdag.core.spi;

import io.digdag.core.config.Config;

public interface TaskExecutor
{
    TaskResult run();

    Config getStateParams();
}
