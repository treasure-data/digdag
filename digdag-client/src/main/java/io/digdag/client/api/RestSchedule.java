package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestSchedule.class)
public interface RestSchedule
{
    Id getId();

    IdAndName getProject();

    IdAndName getWorkflow();

    Instant getNextRunTime();

    OffsetDateTime getNextScheduleTime();

    Optional<Instant> getDisabledAt();

    Optional<Instant> getStartDate();

    Optional<Instant> getEndDate();

    static ImmutableRestSchedule.Builder builder()
    {
        return ImmutableRestSchedule.builder();
    }
}
