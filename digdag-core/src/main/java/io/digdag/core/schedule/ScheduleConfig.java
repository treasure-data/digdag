package io.digdag.core.schedule;

import io.digdag.client.config.Config;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableScheduleConfig.class)
@JsonDeserialize(as = ImmutableScheduleConfig.class)
public interface ScheduleConfig
{
    boolean getEnabled();

    static ImmutableScheduleConfig.Builder defaultBuilder()
    {
        return ImmutableScheduleConfig.builder()
            .enabled(true);
    }

    static ScheduleConfig convertFrom(Config config)
    {
        return defaultBuilder()
            .enabled(config.get("schedule.enabled", boolean.class, true))
            .build();
    }
}
