package io.digdag.client.api;

import java.time.Instant;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSchedule.class)
@JsonDeserialize(as = ImmutableRestSchedule.class)
public abstract class RestSchedule
{
    public abstract long getId();

    public abstract IdName getRepository();

    public abstract String getWorkflowName();

    // TODO add timezone here so that Check and ShowSchedule can show "next session time" in this timezone

    // unix timestamp in seconds
    public abstract long getNextRunTime();

    // unix timestamp in seconds
    public abstract long getNextScheduleTime();

    public static ImmutableRestSchedule.Builder builder()
    {
        return ImmutableRestSchedule.builder();
    }
}
