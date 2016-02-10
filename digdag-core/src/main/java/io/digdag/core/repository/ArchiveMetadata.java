package io.digdag.core.repository;

import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableArchiveMetadata.class)
@JsonDeserialize(as = ImmutableArchiveMetadata.class)
public abstract class ArchiveMetadata
{
    public static final String FILE_NAME = ".digdag.yml";

    @JsonProperty("workflows")
    public abstract WorkflowDefinitionList getWorkflowList();

    @JsonProperty("timezone")
    public abstract ZoneId getDefaultTimeZone();

    @JsonProperty("params")
    public abstract Config getDefaultParams();

    public static ImmutableArchiveMetadata.Builder builder()
    {
        return ImmutableArchiveMetadata.builder();
    }

    public static ArchiveMetadata of(WorkflowDefinitionList workflows, Config defaultParams, ZoneId defaultTimeZone)
    {
        return builder()
            .workflowList(workflows)
            .defaultTimeZone(defaultTimeZone)
            .defaultParams(defaultParams)
            .build();
    }
}
