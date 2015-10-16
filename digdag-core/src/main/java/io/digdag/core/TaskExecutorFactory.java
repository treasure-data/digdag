package io.digdag.core;

public interface TaskExecutorFactory
{
    String getType();

    TaskExecutor newTaskExecutor(ConfigSource config, ConfigSource params, ConfigSource state);
}
