package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestWorkflowDefinition.class)
public interface RestWorkflowDefinition
{
    Id getId();

    String getName();

    IdAndName getProject();

    String getRevision();

    Config getConfig();

    static ImmutableRestWorkflowDefinition.Builder builder()
    {
        return ImmutableRestWorkflowDefinition.builder();
    }
}
