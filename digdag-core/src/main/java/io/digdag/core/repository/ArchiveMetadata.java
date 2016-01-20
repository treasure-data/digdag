package io.digdag.core.repository;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.immutables.value.Value;
import io.digdag.spi.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableArchiveMetadata.class)
@JsonDeserialize(as = ImmutableArchiveMetadata.class)
public abstract class ArchiveMetadata
{
    public static final String FILE_NAME = ".digdag.yml";

    @JsonProperty("workflows")
    public abstract WorkflowSourceList getWorkflowList();

    @JsonProperty("schedules")
    public abstract ScheduleSourceList getScheduleList();

    @JsonProperty("params")
    public abstract Config getDefaultParams();

    public static ImmutableArchiveMetadata.Builder builder()
    {
        return ImmutableArchiveMetadata.builder();
    }

    public static ArchiveMetadata of(WorkflowSourceList workflows, ScheduleSourceList schedules, Config defaultParams)
    {
        return builder()
            .workflowList(workflows)
            .scheduleList(schedules)
            .defaultParams(defaultParams)
            .build();
    }
}
