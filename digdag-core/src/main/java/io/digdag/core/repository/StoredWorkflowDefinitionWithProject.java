package io.digdag.core.repository;

import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredWorkflowDefinitionWithProject.class)
@JsonDeserialize(as = ImmutableStoredWorkflowDefinitionWithProject.class)
public abstract class StoredWorkflowDefinitionWithProject
        extends StoredWorkflowDefinition
{
    public abstract StoredProject getProject();

    public abstract Config getRevisionDefaultParams();

    public abstract String getRevisionName();

    public static StoredWorkflowDefinitionWithProject of(
            StoredWorkflowDefinition def, StoredProject proj, Revision rev)
    {
        return ImmutableStoredWorkflowDefinitionWithProject.builder()
            .from(def)
            .project(proj)
            .revisionDefaultParams(rev.getDefaultParams().deepCopy())
            .revisionName(rev.getName())
            .build();
    }
}
