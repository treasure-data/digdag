package io.digdag.core.repository;

import java.time.ZoneId;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredWorkflowDefinitionWithRepository.class)
@JsonDeserialize(as = ImmutableStoredWorkflowDefinitionWithRepository.class)
public abstract class StoredWorkflowDefinitionWithRepository
        extends StoredWorkflowDefinition
{
    public abstract StoredRepository getRepository();

    public abstract Config getRevisionDefaultParams();

    public abstract String getRevisionName();

    public static StoredWorkflowDefinitionWithRepository of(
            StoredWorkflowDefinition def, StoredRepository repo, Revision rev)
    {
        return ImmutableStoredWorkflowDefinitionWithRepository.builder()
            .from(def)
            .repository(repo)
            .revisionDefaultParams(rev.getDefaultParams().deepCopy())
            .revisionName(rev.getName())
            .build();
    }
}
