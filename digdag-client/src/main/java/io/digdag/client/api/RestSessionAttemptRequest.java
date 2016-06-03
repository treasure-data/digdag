package io.digdag.client.api;

import java.util.List;
import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@Value.Enclosing
@JsonSerialize(as = ImmutableRestSessionAttemptRequest.class)
@JsonDeserialize(as = ImmutableRestSessionAttemptRequest.class)
public abstract class RestSessionAttemptRequest
{
    public abstract long getWorkflowId();

    public abstract Instant getSessionTime();

    public abstract Optional<String> getRetryAttemptName();

    public abstract Optional<Resume> getResume();

    public abstract Config getParams();

    @Value.Immutable
    @JsonSerialize(as = ImmutableRestSessionAttemptRequest.Resume.class)
    @JsonDeserialize(as = ImmutableRestSessionAttemptRequest.Resume.class)
    public static abstract class Resume
    {
        public abstract long getAttemptId();

        @JsonProperty("from")
        public abstract String getFromTaskNamePattern();

        public static Resume ofFromTaskNamePattern(long attemptId, String fromTaskNamePattern)
        {
            return builder()
                .attemptId(attemptId)
                .fromTaskNamePattern(fromTaskNamePattern)
                .build();
        }

        public static ImmutableRestSessionAttemptRequest.Resume.Builder builder()
        {
            return ImmutableRestSessionAttemptRequest.Resume.builder();
        }
    }

    public static ImmutableRestSessionAttemptRequest.Builder builder()
    {
        return ImmutableRestSessionAttemptRequest.builder();
    }
}
