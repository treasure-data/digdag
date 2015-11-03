package io.digdag.core;

import io.digdag.core.config.Config;

public interface TaskExecutor
{
    TaskResult run();

    Config getState();
}
