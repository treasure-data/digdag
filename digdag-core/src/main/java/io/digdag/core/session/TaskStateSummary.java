package io.digdag.core.session;

import java.time.Instant;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskStateSummary.class)
@JsonDeserialize(as = ImmutableTaskStateSummary.class)
public abstract class TaskStateSummary
{
    public abstract long getId();

    public abstract Optional<Long> getParentId();

    public abstract TaskStateCode getState();

    public abstract Instant getUpdatedAt();
}
