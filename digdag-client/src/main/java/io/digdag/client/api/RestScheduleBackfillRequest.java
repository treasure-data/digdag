package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableRestScheduleBackfillRequest.class)
@JsonDeserialize(as = ImmutableRestScheduleBackfillRequest.class)
public abstract class RestScheduleBackfillRequest
{
    // unix timestamp in seconds
    public abstract long getFromTime();

    public abstract boolean getDryRun();

    public abstract String getAttemptName();

    public static ImmutableRestScheduleBackfillRequest.Builder builder()
    {
        return ImmutableRestScheduleBackfillRequest.builder();
    }

    @Value.Check
    protected void check()
    {
    }
}
