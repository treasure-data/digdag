package io.digdag.standards.command.ecs;

import io.digdag.client.config.ConfigException;

public interface EcsClientFactory
{
    EcsClient createClient(EcsClientConfig ecsClientConfig)
            throws ConfigException;
}
