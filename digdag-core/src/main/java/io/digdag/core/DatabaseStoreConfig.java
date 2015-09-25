package io.digdag.core;

import org.immutables.value.Value;
import com.google.common.base.*;
import com.google.common.collect.*;

@Value.Immutable
public abstract class DatabaseStoreConfig
{
    public abstract String getType();

    public abstract String getUrl();

    public static ImmutableDatabaseStoreConfig.Builder builder()
    {
        return ImmutableDatabaseStoreConfig.builder();
    }
}
