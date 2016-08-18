package io.digdag.client.api;

import java.util.UUID;
import java.time.Instant;
import java.time.OffsetDateTime;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSessionAttempt.class)
@JsonDeserialize(as = ImmutableRestSessionAttempt.class)
public interface RestSessionAttempt
{
    long getId();

    IdName getProject();

    //Optional<String> getRevision();

    NameOptionalId getWorkflow();

    long getSessionId();

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
