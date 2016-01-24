package io.digdag.client.api;

import java.time.Instant;
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

    public abstract long getSessionTime();

    public abstract String getAttemptName();

    public abstract boolean getSuccess();

    public abstract boolean getCancelRequested();

    public abstract Config getParams();

    public abstract Instant getCreatedAt();

    public static ImmutableRestSession.Builder builder()
    {
        return ImmutableRestSession.builder();
    }
}
