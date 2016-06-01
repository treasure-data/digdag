package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import org.immutables.value.Value;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Value.Immutable
@Value.Enclosing
@JsonSerialize(as = ImmutableRestSession.class)
@JsonDeserialize(as = ImmutableRestSession.class)
public abstract class RestSession
{
    public abstract long getId();

    public abstract IdName getProject();

    public abstract NameOptionalId getWorkflow();

    public abstract UUID getSessionUuid();

    public abstract OffsetDateTime getSessionTime();

    public abstract Optional<Attempt> getLastAttempt();

    @Value.Immutable
    @JsonSerialize(as = ImmutableRestSession.Attempt.class)
    @JsonDeserialize(as = ImmutableRestSession.Attempt.class)
    public static abstract class Attempt {

        public abstract long getId();

        public abstract Optional<String> getRetryAttemptName();

        public abstract boolean getDone();

        public abstract boolean getSuccess();

        public abstract boolean getCancelRequested();

        public abstract Config getParams();

        public abstract Instant getCreatedAt();

        public static ImmutableRestSession.Attempt.Builder builder()
        {
            return ImmutableRestSession.Attempt.builder();
        }
    }

    public static ImmutableRestSession.Builder builder()
    {
        return ImmutableRestSession.builder();
    }
}
