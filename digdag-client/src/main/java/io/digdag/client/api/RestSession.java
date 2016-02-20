package io.digdag.client.api;

import java.util.UUID;
import java.time.Instant;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSession.class)
@JsonDeserialize(as = ImmutableRestSession.class)
public abstract class RestSession
{
    public abstract long getId();

    public abstract IdName getRepository();

    //public abstract Optional<String> getRevision();

    public abstract String getWorkflowName();

    public abstract UUID getSessionUuid();

    public abstract long getSessionTime();

    public abstract Optional<String> getRetryAttemptName();

    public abstract boolean getDone();

    public abstract boolean getSuccess();

    public abstract boolean getCancelRequested();

    public abstract Config getParams();

    public abstract Instant getCreatedAt();

    public static ImmutableRestSession.Builder builder()
    {
        return ImmutableRestSession.builder();
    }
}
