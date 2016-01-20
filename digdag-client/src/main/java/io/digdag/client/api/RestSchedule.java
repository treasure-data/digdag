package io.digdag.client.api;

import java.util.Date;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.spi.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSchedule.class)
@JsonDeserialize(as = ImmutableRestSchedule.class)
public abstract class RestSchedule
{
    public abstract long getId();

    public abstract IdName getWorkflow();

    public abstract Config getConfig();

    // unix timestamp in seconds
    public abstract long getNextRunTime();

    // unix timestamp in seconds
    public abstract long getNextScheduleTime();

    public abstract Date getCreatedAt();

    public abstract Date getUpdatedAt();

    public static ImmutableRestSchedule.Builder builder()
    {
        return ImmutableRestSchedule.builder();
    }
}
