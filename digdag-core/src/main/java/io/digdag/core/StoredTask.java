package io.digdag.core;

import java.util.List;
import java.util.Map;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@ImmutableAbstractStyle
@JsonSerialize(as = ImmutableStoredTask.class)
@JsonDeserialize(as = ImmutableStoredTask.class)
public abstract class StoredTask
        extends Task
{
    public abstract long getId();

    public abstract int getSiteId();

    public abstract List<Long> getUpstreams();  // list of task_id

    public abstract Date getUpdatedAt();

    public abstract Optional<Date> getRetryAt();

    public abstract ConfigSource getStateParams();

    public abstract Optional<TaskReport> getReport();

    public abstract Optional<ConfigSource> getError();  // TODO error class

    @Value.Check
    protected void check()
    {
        checkState(!getError().isPresent() || !getError().get().isEmpty(), "error must not be empty if not null");
    }
}
