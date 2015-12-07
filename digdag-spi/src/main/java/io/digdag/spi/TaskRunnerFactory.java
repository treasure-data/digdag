package io.digdag.spi;

public interface TaskRunnerFactory
{
    String getType();

    TaskRunner newTaskExecutor(TaskRequest request);
}
