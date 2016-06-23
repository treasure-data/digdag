package io.digdag.core.session;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableStoredSessionAttempt.class)
public abstract class StoredSessionAttempt
        extends SessionAttempt
{
    public abstract long getId();

    public abstract AttemptStateFlags getStateFlags();

    public abstract long getSessionId();

    public abstract Instant getCreatedAt();

    public static StoredSessionAttempt copyOf(StoredSessionAttempt o) {
        return ImmutableStoredSessionAttempt.builder().from(o).build();
    }
}
