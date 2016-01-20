package io.digdag.spi;

import io.digdag.client.config.Config;

public interface TaskRunner
{
    TaskResult run();

    Config getStateParams();
}
