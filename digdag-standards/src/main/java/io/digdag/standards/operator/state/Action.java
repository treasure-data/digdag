package io.digdag.standards.operator.state;

@FunctionalInterface
public interface Action
{
    void perform(TaskState state)
            throws Exception;
}
