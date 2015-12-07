package io.digdag.spi;

import io.digdag.spi.config.Config;

public interface TaskRunner
{
    TaskResult run();

    Config getStateParams();
}
