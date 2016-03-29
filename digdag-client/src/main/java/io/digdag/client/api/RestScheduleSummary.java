package io.digdag.client.api;

import java.time.Instant;
import java.time.OffsetDateTime;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestScheduleSummary.class)
@JsonDeserialize(as = ImmutableRestScheduleSummary.class)
public abstract class RestScheduleSummary
{
    public abstract long getId();

    public abstract String getWorkflowName();

    public abstract Instant getNextRunTime();

    public abstract OffsetDateTime getNextScheduleTime();

    public abstract Instant getCreatedAt();

    public abstract Instant getUpdatedAt();

    public static ImmutableRestScheduleSummary.Builder builder()
    {
        return ImmutableRestScheduleSummary.builder();
    }
}
