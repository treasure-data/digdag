package io.digdag.core.repository;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableScheduleSource.class)
public abstract class ScheduleSource
{
    public abstract String getName();

    public abstract Config getConfig();

    public static ImmutableScheduleSource.Builder scheduleSourceBuilder()
    {
        return ImmutableScheduleSource.builder();
    }

    public static ScheduleSource of(String name, Config config)
    {
        return scheduleSourceBuilder()
            .name(name)
            .config(config)
            .build();
    }

    @Value.Check
    protected void check()
    {
        // TODO check name
        //   must start with -
    }
}
