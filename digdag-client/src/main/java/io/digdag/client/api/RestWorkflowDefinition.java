package io.digdag.client.api;

import java.time.Instant;
import java.time.OffsetDateTime;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestWorkflowDefinition.class)
@JsonDeserialize(as = ImmutableRestWorkflowDefinition.class)
public abstract class RestWorkflowDefinition
{
    public abstract long getId();

    @JsonProperty("package")
    public abstract String getPackageName();

    public abstract String getName();

    public abstract IdName getRepository();

    public abstract String getRevision();

    public abstract Config getConfig();

    public static ImmutableRestWorkflowDefinition.Builder builder()
    {
        return ImmutableRestWorkflowDefinition.builder();
    }
}
