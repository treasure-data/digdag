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
    public abstract Date getNextRunTime();

    public abstract Date getNextScheduleTime();

    public static ImmutableScheduleTime.Builder builder()
    {
        return ImmutableScheduleTime.builder();
    }

    public static ScheduleTime of(Date nextRunTime, Date nextScheduleTime)
    {
        return builder()
            .nextRunTime(nextRunTime)
            .nextScheduleTime(nextScheduleTime)
            .build();
    }

    @Value.Check
    protected void check()
    {
        checkState(getNextRunTime().getTime() % (1000) == 0, "Next run time must be aligned with second");
        checkState(getNextScheduleTime().getTime() % (60*1000) == 0, "Next schedule time must be aligned with minute");
    }
}
