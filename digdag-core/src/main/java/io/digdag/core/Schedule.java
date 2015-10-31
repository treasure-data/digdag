package io.digdag.core;

import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSchedule.class)
public abstract class Schedule
{
    public abstract int getWorkflowId();

    public abstract ScheduleType getScheduleType();

    public abstract ConfigSource getConfig();

    public abstract Date getNextRunTime();

    public abstract Date getNextScheduleTime();

    public static ImmutableSchedule.Builder scheduleBuilder()
    {
        return ImmutableSchedule.builder();
    }

    public static Schedule of(int workflowId, ConfigSource config, Date nextRunTime, Date nextScheduleTime)
    {
        return scheduleBuilder()
            .workflowId(workflowId)
            .scheduleType(new ScheduleType(0))
            .config(config)
            .nextRunTime(nextRunTime)
            .nextScheduleTime(nextScheduleTime)
            .build();
    }

    public static Schedule ofSla(int workflowId, ConfigSource config, Date nextRunTime, Date nextScheduleTime)
    {
        return scheduleBuilder()
            .workflowId(workflowId)
            .scheduleType(new ScheduleType(ScheduleType.SLA_TASK))
            .config(config)
            .nextRunTime(nextRunTime)
            .nextScheduleTime(nextScheduleTime)
            .build();
    }
}
