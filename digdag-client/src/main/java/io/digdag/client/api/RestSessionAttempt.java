package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestSessionAttempt.class)
public interface RestSessionAttempt
{
    Id getId();

    int getIndex();

    IdAndName getProject();

    //Optional<String> getRevision();

    NameOptionalId getWorkflow();

    Id getSessionId();

    UUID getSessionUuid();

    OffsetDateTime getSessionTime();

    Optional<String> getRetryAttemptName();

    boolean getDone();

    boolean getSuccess();

    boolean getCancelRequested();

    Config getParams();

    Instant getCreatedAt();

    Optional<Instant> getFinishedAt();

    static ImmutableRestSessionAttempt.Builder builder()
    {
        return ImmutableRestSessionAttempt.builder();
    }
}
