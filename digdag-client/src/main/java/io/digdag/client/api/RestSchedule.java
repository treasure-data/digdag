package io.digdag.client.api;

import java.time.Instant;
import java.time.OffsetDateTime;
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
    public abstract int getId();

    public abstract IdName getRepository();

    public abstract String getWorkflowName();

    public abstract Instant getNextRunTime();

    public abstract OffsetDateTime getNextScheduleTime();

    public static ImmutableRestSchedule.Builder builder()
    {
        return ImmutableRestSchedule.builder();
    }
}
