package io.digdag.core.session;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import java.time.Instant;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableStoredDelayedSessionAttempt.class)
public abstract class StoredDelayedSessionAttempt
{
    public abstract long getAttemptId();

    public abstract Optional<Long> getDependentSessionId();

    public abstract Instant getNextRunTime();

    public abstract Instant getUpdatedAt();
}
