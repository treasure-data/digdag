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
public abstract class RestSessionAttempt
{
    public abstract long getId();

    public abstract IdName getProject();

    //public abstract Optional<String> getRevision();

    public abstract NameOptionalId getWorkflow();

    public abstract long getSessionId();

    public abstract UUID getSessionUuid();

    public abstract OffsetDateTime getSessionTime();

    public abstract Optional<String> getRetryAttemptName();

    public abstract boolean getDone();

    public abstract boolean getSuccess();

    public abstract boolean getCancelRequested();

    public abstract Config getParams();

    public abstract Instant getCreatedAt();

    public static ImmutableRestSessionAttempt.Builder builder()
    {
        return ImmutableRestSessionAttempt.builder();
    }
}
