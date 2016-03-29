package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableRestScheduleSkipRequest.class)
@JsonDeserialize(as = ImmutableRestScheduleSkipRequest.class)
public abstract class RestScheduleSkipRequest
{
    public abstract Optional<Integer> getCount();

    public abstract Optional<Instant> getFromTime();

    public abstract Optional<Instant> getNextTime();

    public abstract Optional<Instant> getNextRunTime();

    public abstract boolean getDryRun();

    public static ImmutableRestScheduleSkipRequest.Builder builder()
    {
        return ImmutableRestScheduleSkipRequest.builder();
    }

    @Value.Check
    protected void check()
    {
        checkState(getNextTime().isPresent() || (getCount().isPresent() && getFromTime().isPresent()), "nextTime or (fromTime and count) are required");
    }
}
