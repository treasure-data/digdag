package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSessionAttemptPrepareResult.class)
@JsonDeserialize(as = ImmutableRestSessionAttemptPrepareResult.class)
public abstract class RestSessionAttemptPrepareResult
{
    public abstract long getWorkflowId();

    public abstract String getRevision();

    public abstract Instant getSessionTime();

    public static ImmutableRestSessionAttemptPrepareResult.Builder builder()
    {
        return ImmutableRestSessionAttemptPrepareResult.builder();
    }
}
