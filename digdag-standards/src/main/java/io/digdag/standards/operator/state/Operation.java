package io.digdag.standards.operator.state;

@FunctionalInterface
public interface Operation<R>
{
    R perform(TaskState state)
            throws Exception;
}
