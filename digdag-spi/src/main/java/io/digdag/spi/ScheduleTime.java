package io.digdag.spi;

import java.time.Instant;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;

@Value.Immutable
@JsonSerialize(as = ImmutableScheduleTime.class)
@JsonDeserialize(as = ImmutableScheduleTime.class)
public interface ScheduleTime
{
    /**
     * Actual execution time of a scheduled attempt.
     */
    Instant getRunTime();

    /**
     * The session_time variable of a scheduled attempt.
     *
     * A session attempt has session_time variable which may not be same with
     * actual run time because a lot of scheduled workflows has a target time.
     * Target time is usually exact time (e.g. 00:00:00 every day), while actual
     * run time usually has some delay (e.g. 00:00:13) because a workflow may
     * want to wait for other components to be prepared before starting, session
     * could be created using backfill, or queuing of an attempt may be delayed.
     */
    Instant getTime();

    Optional<Instant> getStartDate();

    Optional<Instant> getEndDate();

    static ScheduleTime of(Instant time, Instant runTime, Optional<Instant> startDate, Optional<Instant> endDate)
    {
        return ImmutableScheduleTime.builder()
            .time(time)
            .runTime(runTime)
            .startDate(startDate)
            .endDate(endDate)
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
