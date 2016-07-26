package io.digdag.spi;

import io.digdag.client.config.Config;

public interface TaskQueueFactory
{
    String getType();

    TaskQueueServer newServer(Config systemConfig);

    TaskQueueClient newDirectClient(Config systemConfig);
}
