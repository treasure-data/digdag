package io.digdag.core.session;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import java.util.UUID;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredSessionWithLastAttempt.class)
@JsonDeserialize(as = ImmutableStoredSessionWithLastAttempt.class)
public abstract class StoredSessionWithLastAttempt
        extends StoredSession
{
    public abstract int getSiteId();

    public abstract StoredSessionAttempt getLastAttempt();
}
