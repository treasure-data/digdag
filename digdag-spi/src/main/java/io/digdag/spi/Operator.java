package io.digdag.spi;

import io.digdag.client.config.Config;

public interface Operator
{
    TaskResult run();
}
