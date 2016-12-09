package io.digdag.client.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestScheduleSkipRequest.class)
public interface RestScheduleSkipRequest
{
    Optional<Integer> getCount();

    Optional<Instant> getFromTime();

    Optional<LocalTimeOrInstant> getNextTime();

    Optional<Instant> getNextRunTime();

    boolean getDryRun();

    static ImmutableRestScheduleSkipRequest.Builder builder()
    {
        return ImmutableRestScheduleSkipRequest.builder();
    }
}
