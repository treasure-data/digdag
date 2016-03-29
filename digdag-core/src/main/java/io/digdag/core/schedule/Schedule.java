package io.digdag.core.schedule;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSchedule.class)
public abstract class Schedule
{
    public abstract String getWorkflowName();

    public abstract long getWorkflowDefinitionId();

    public abstract Instant getNextRunTime();

    public abstract Instant getNextScheduleTime();

    public abstract ZoneId getTimeZone();

    public static Schedule of(String workflowName, long workflowDefinitionId, Instant nextRunTime, Instant nextScheduleTime)
    {
        return ImmutableSchedule.builder()
            .workflowName(workflowName)
            .workflowDefinitionId(workflowDefinitionId)
            .nextRunTime(nextRunTime)
            .nextScheduleTime(nextScheduleTime)
            .build();
    }
}
