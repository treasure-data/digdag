package io.digdag.core.session;

import java.time.Instant;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSessionAttemptSummary.class)
@JsonDeserialize(as = ImmutableSessionAttemptSummary.class)
public abstract class SessionAttemptSummary
{
    public abstract long getId();

    public abstract long getSessionId();

    public abstract int getIndex();

    public abstract AttemptStateFlags getStateFlags();
}

