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
}
