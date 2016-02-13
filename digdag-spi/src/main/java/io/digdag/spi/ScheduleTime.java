package io.digdag.spi;

import java.time.Instant;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableScheduleTime.class)
@JsonDeserialize(as = ImmutableScheduleTime.class)
public abstract class ScheduleTime
{
    public abstract Instant getRunTime();

    public abstract Instant getTime();

    public static ImmutableScheduleTime.Builder builder()
    {
        return ImmutableScheduleTime.builder();
    }

    public static ScheduleTime of(Instant time, Instant runTime)
    {
        return builder()
            .time(time)
            .runTime(runTime)
            .build();
    }

    public static ScheduleTime runNow(Instant time)
    {
        return builder()
            .time(time)
            .runTime(Instant.now())
            .build();
    }

    @Value.Check
    protected void check()
    {
        //checkState(getTime().getEpochSecond() % 60 == 0, "Schedule time must be aligned with minute");
        //checkState(getTime().getNano() == 0, "Schedule time must be aligned with second");
        //checkState(getRunTime().getNano() == 0, "Run time must be aligned with second");
    }
}
