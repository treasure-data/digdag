package io.digdag.core.session;

import java.time.Instant;
import com.google.common.base.Optional;
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

    public abstract int getIndex();

    public abstract Instant getCreatedAt();

    public abstract Optional<Instant> getFinishedAt();

    public static StoredSessionAttempt copyOf(StoredSessionAttempt o)
    {
        return ImmutableStoredSessionAttempt.builder().from(o).build();
    }
}
