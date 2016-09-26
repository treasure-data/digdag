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
public interface RestScheduleSummary
{
    int getId();

    NameLongId getWorkflow();

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
