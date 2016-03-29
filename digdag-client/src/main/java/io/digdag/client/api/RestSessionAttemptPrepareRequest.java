package io.digdag.client.api;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSessionAttemptPrepareRequest.class)
@JsonDeserialize(as = ImmutableRestSessionAttemptPrepareRequest.class)
public abstract class RestSessionAttemptPrepareRequest
{
    public abstract String getRepositoryName();

    public abstract Optional<String> getRevision();

    public abstract String getWorkflowName();

    public abstract LocalTimeOrInstant getSessionTime();

    public abstract Optional<SessionTimeTruncate> getSessionTimeTruncate();

    public static ImmutableRestSessionAttemptPrepareRequest.Builder builder()
    {
        return ImmutableRestSessionAttemptPrepareRequest.builder();
    }
}
