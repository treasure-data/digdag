package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestWorkflowDefinitionCollection.class)
public interface RestWorkflowDefinitionCollection
{
    List<RestWorkflowDefinition> getWorkflows();

    static ImmutableRestWorkflowDefinitionCollection.Builder builder()
    {
        return ImmutableRestWorkflowDefinitionCollection.builder();
    }
}
