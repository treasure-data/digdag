package io.digdag.core;

import java.util.List;
import java.util.Date;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableScheduleTime.class)
@JsonDeserialize(as = ImmutableScheduleTime.class)
public abstract class ScheduleTime
{
    public abstract Date getRunTime();

    public abstract Date getScheduleTime();

    public static ImmutableScheduleTime.Builder builder()
    {
        return ImmutableScheduleTime.builder();
    }

    public static ScheduleTime of(Date nextRunTime, Date nextScheduleTime)
    {
        return builder()
            .runTime(nextRunTime)
            .scheduleTime(nextScheduleTime)
            .build();
    }

    @Value.Check
    protected void check()
    {
        checkState(getRunTime().getTime() % (1000) == 0, "Run time must be aligned with second");
        checkState(getScheduleTime().getTime() % (60*1000) == 0, "Schedule time must be aligned with minute");
    }
}
