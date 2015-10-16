package io.digdag.core;

public interface TaskExecutor
{
    TaskResult run();

    ConfigSource getState();
}
