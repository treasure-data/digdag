package io.digdag.core.spi;

import io.digdag.core.spi.config.Config;

public interface TaskRunner
{
    TaskResult run();

    Config getStateParams();
}
