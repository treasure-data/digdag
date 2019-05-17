package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestScheduleSummary.class)
public interface RestScheduleSummary
{
    Id getId();

    IdAndName getProject();

    IdAndName getWorkflow();

    Instant getNextRunTime();

    OffsetDateTime getNextScheduleTime();

    Instant getCreatedAt();

    Instant getUpdatedAt();

    Optional<Instant> getDisabledAt();

    static ImmutableRestScheduleSummary.Builder builder()
    {
        return ImmutableRestScheduleSummary.builder();
    }
}
