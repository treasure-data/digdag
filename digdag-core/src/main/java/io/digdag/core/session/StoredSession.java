package io.digdag.core.session;

import java.util.UUID;
import java.time.Instant;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredSession.class)
@JsonDeserialize(as = ImmutableStoredSession.class)
public abstract class StoredSession
        extends Session
{
    public abstract long getId();

    public abstract UUID getUuid();

    public abstract long getLastAttemptId();
}
