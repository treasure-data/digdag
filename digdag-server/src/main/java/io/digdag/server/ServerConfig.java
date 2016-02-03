package io.digdag.server;

import org.immutables.value.Value;

@Value.Immutable
public abstract class ServerConfig
{
    public static ImmutableServerConfig.Builder builder()
    {
        return ImmutableServerConfig.builder();
    }
}
