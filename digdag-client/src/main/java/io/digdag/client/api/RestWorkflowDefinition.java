package io.digdag.client.api;

import java.time.Instant;
import java.time.OffsetDateTime;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestWorkflowDefinition.class)
@JsonDeserialize(as = ImmutableRestWorkflowDefinition.class)
public interface RestWorkflowDefinition
{
    long getId();

    String getName();

    IdName getProject();

    String getRevision();

    Config getConfig();

    static ImmutableRestWorkflowDefinition.Builder builder()
    {
        return ImmutableRestWorkflowDefinition.builder();
    }
}
