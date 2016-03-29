package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSessionAttemptRequest.class)
@JsonDeserialize(as = ImmutableRestSessionAttemptRequest.class)
public abstract class RestSessionAttemptRequest
{
    public abstract long getWorkflowId();

    public abstract Instant getSessionTime();

    public abstract Optional<String> getRetryAttemptName();

    public abstract Config getParams();

    //@JsonProperty("from")
    //public abstract Optional<String> getFromTaskName();

    public static ImmutableRestSessionAttemptRequest.Builder builder()
    {
        return ImmutableRestSessionAttemptRequest.builder();
    }
}
