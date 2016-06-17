package io.digdag.spi;

import java.time.Instant;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonSerialize(as = ImmutableScheduleTime.class)
@JsonDeserialize(as = ImmutableScheduleTime.class)
public interface ScheduleTime
{
    Instant getRunTime();

    Instant getTime();

    static ScheduleTime of(Instant time, Instant runTime)
    {
        return ImmutableScheduleTime.builder()
            .time(time)
            .runTime(runTime)
            .build();
    }

    static ScheduleTime runNow(Instant time)
    {
        return ImmutableScheduleTime.builder()
            .time(time)
            .runTime(Instant.now())
            .build();
    }

    static Instant alignedNow()
    {
        return Instant.ofEpochSecond(Instant.now().getEpochSecond());
    }
}
