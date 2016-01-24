package io.digdag.core.workflow;

import java.time.Instant;
import java.time.ZoneId;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableAttemptRequest.class)
@JsonDeserialize(as = ImmutableAttemptRequest.class)
public abstract class AttemptRequest
{
    public abstract int getRepositoryId();

    public abstract String getWorkflowName();

    public abstract Instant getInstant();

    public abstract Optional<String> getRetryAttemptName();

    public abstract ZoneId getDefaultTimeZone();

    public abstract Config getDefaultParams();

    public abstract Config getOverwriteParams();

    public abstract Optional<Long> getStoredWorkflowDefinitionId();

    public static ImmutableAttemptRequest.Builder builder()
    {
        return ImmutableAttemptRequest.builder();
    }

    @Value.Check
    protected void check()
    {
        // TODO if getRetryAttemptName().isPresent, get() should not be empty
    }
}
