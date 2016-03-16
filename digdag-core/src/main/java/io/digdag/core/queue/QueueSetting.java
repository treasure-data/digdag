package io.digdag.core.queue;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableQueueSetting.class)
public abstract class QueueSetting
{
    public abstract String getName();

    public abstract Config getConfig();

    public static ImmutableQueueSetting.Builder queueSettingBuilder()
    {
        return ImmutableQueueSetting.builder();
    }

    public static QueueSetting of(String name, Config config)
    {
        return queueSettingBuilder()
            .name(name)
            .config(config)
            .build();
    }
}
