package io.digdag.client.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestWorkflowDefinition.class)
public interface RestWorkflowDefinition
{
    Id getId();

    String getName();

    IdAndName getProject();

    String getRevision();

    @JsonProperty("timezone")
    ZoneId getTimeZone();

    Config getConfig();

    static ImmutableRestWorkflowDefinition.Builder builder()
    {
        return ImmutableRestWorkflowDefinition.builder();
    }
}
