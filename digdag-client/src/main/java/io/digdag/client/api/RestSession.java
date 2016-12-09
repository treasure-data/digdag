package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import org.immutables.value.Value;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

@Value.Immutable
@Value.Enclosing
@JsonDeserialize(as = ImmutableRestSession.class)
public interface RestSession
{
    Id getId();

    IdAndName getProject();

    NameOptionalId getWorkflow();

    UUID getSessionUuid();

    OffsetDateTime getSessionTime();

    Optional<Attempt> getLastAttempt();

    @Value.Immutable
    @JsonDeserialize(as = ImmutableRestSession.Attempt.class)
    interface Attempt
    {
        Id getId();

        Optional<String> getRetryAttemptName();

        boolean getDone();

        boolean getSuccess();

        boolean getCancelRequested();

        Config getParams();

        Instant getCreatedAt();

        Optional<Instant> getFinishedAt();

        static ImmutableRestSession.Attempt.Builder builder()
        {
            return ImmutableRestSession.Attempt.builder();
        }
    }

    static ImmutableRestSession.Builder builder()
    {
        return ImmutableRestSession.builder();
    }
}
