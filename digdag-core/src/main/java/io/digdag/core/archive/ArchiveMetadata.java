package io.digdag.core.archive;

import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.immutables.value.Value;
import io.digdag.core.repository.WorkflowDefinitionList;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableArchiveMetadata.class)
@JsonDeserialize(as = ImmutableArchiveMetadata.class)
public abstract class ArchiveMetadata
{
    @JsonProperty("workflows")
    public abstract WorkflowDefinitionList getWorkflowList();

    @JsonProperty("params")
    public abstract Config getDefaultParams();

    public static ArchiveMetadata of(WorkflowDefinitionList workflows, Config defaultParams)
    {
        return ImmutableArchiveMetadata.builder()
            .workflowList(workflows)
            .defaultParams(defaultParams)
            .build();
    }
}
