package io.digdag.client.api;

import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestWorkflowDefinition.class)
@JsonDeserialize(as = ImmutableRestWorkflowDefinition.class)
public abstract class RestWorkflowDefinition
{
    public abstract String getName();

    public abstract IdName getRepository();

    public abstract String getRevision();

    public abstract Config getConfig();

    // unix timestamp in seconds
    public abstract Optional<Long> getNextScheduleTime();

    // unix timestamp in seconds
    public abstract Optional<Long> getNextRunTime();

    public static ImmutableRestWorkflowDefinition.Builder builder()
    {
        return ImmutableRestWorkflowDefinition.builder();
    }
}
