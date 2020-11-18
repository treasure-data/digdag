package io.digdag.spi;

public interface Operator
{
    TaskResult run();

    default void cleanup(TaskRequest request)
    { }
}
