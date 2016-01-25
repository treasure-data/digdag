package io.digdag.core.session;

import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskAttemptSummary.class)
@JsonDeserialize(as = ImmutableTaskAttemptSummary.class)
public abstract class TaskAttemptSummary
{
    public abstract long getId();

    public abstract long getAttemptId();

    public abstract TaskStateCode getState();
}
