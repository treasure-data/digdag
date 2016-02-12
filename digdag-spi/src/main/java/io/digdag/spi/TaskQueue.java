package io.digdag.spi;

public interface TaskQueue
{
    public TaskQueueServer getServer();

    public TaskQueueClient getDirectClientIfSupported();
}
