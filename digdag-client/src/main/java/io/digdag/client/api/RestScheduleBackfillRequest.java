package io.digdag.client.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.time.Instant;
import org.immutables.value.Value;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestScheduleBackfillRequest.class)
public interface RestScheduleBackfillRequest
{
    Instant getFromTime();

    boolean getDryRun();

    String getAttemptName();

    Optional<Integer> getCount();

    static ImmutableRestScheduleBackfillRequest.Builder builder()
    {
        return ImmutableRestScheduleBackfillRequest.builder();
    }
}
