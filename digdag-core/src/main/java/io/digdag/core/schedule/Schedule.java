package io.digdag.core.schedule;

import java.util.List;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSchedule.class)
public abstract class Schedule
{
    public abstract String getName();

    public abstract int getScheduleSourceId();

    public abstract int getWorkflowSourceId();

    public abstract Date getNextRunTime();

    public abstract Date getNextScheduleTime();

    public static ImmutableSchedule.Builder scheduleBuilder()
    {
        return ImmutableSchedule.builder();
    }

    public static Schedule of(String name, int scheduleSourceId, int workflowSourceId, Date nextRunTime, Date nextScheduleTime)
    {
        return scheduleBuilder()
            .name(name)
            .scheduleSourceId(scheduleSourceId)
            .workflowSourceId(workflowSourceId)
            .nextRunTime(nextRunTime)
            .nextScheduleTime(nextScheduleTime)
            .build();
    }
}
