package io.digdag.core.spi;

public interface TaskRunnerFactory
{
    String getType();

    TaskRunner newTaskExecutor(TaskRequest request);
}
