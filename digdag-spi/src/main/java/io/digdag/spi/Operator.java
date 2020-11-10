package io.digdag.spi;

public interface Operator
{
    TaskResult run();

    default TaskResult cleanup(TaskRequest request)
    {
        return TaskResult.empty(request);
    }
}
