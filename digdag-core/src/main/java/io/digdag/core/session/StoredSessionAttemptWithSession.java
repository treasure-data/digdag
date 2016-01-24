package io.digdag.core.session;

import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredSessionAttempt.class)
@JsonDeserialize(as = ImmutableStoredSessionAttempt.class)
public abstract class StoredSessionAttemptWithSession
        extends StoredSessionAttempt
{
    public abstract int getSiteId();

    public abstract Session getSession();

    public abstract String getRepositoryName();

    //public abstract Optional<String> getRevisionName();
}
