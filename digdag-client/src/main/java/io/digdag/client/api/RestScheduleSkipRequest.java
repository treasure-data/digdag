package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableRestScheduleSkipRequest.class)
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
