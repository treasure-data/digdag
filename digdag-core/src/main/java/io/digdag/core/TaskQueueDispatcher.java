package io.digdag.core;

public interface TaskQueueDispatcher
{
    void dispatch(Action action);
}
