package io.digdag.core.queue;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.core.spi.config.Config;

@JsonDeserialize(as = ImmutableQueueDesc.class)
public abstract class QueueDesc
{
    public abstract String getName();

    public abstract Config getConfig();

    public static ImmutableQueueDesc.Builder queueDescBuilder()
    {
        return ImmutableQueueDesc.queueDescBuilder();
    }

    public static QueueDesc of(String name, Config config)
    {
        return queueDescBuilder()
            .name(name)
            .config(config)
            .build();
    }
}
