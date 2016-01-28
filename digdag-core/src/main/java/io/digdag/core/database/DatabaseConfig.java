package io.digdag.core.database;

import org.immutables.value.Value;

@Value.Immutable
public abstract class DatabaseConfig
{
    public abstract String getType();

    public abstract String getUrl();

    public static ImmutableDatabaseConfig.Builder builder()
    {
        return ImmutableDatabaseConfig.builder();
    }
}
