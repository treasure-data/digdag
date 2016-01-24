package io.digdag.core.session;

import java.time.Instant;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredSessionMonitor.class)
@JsonDeserialize(as = ImmutableStoredSessionMonitor.class)
public abstract class StoredSessionMonitor
        extends SessionMonitor
{
    public abstract long getId();

    public abstract long getAttemptId();

    public abstract Instant getCreatedAt();

    public abstract Instant getUpdatedAt();
}
