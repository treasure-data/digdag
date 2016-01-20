package io.digdag.core.session;

import java.util.List;
import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.spi.TaskReport;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredTask.class)
@JsonDeserialize(as = ImmutableStoredTask.class)
public abstract class StoredTask
        extends Task
{
    public abstract long getId();

    public abstract int getSiteId();

    public abstract List<Long> getUpstreams();  // list of task_id

    public abstract TaskStateFlags getStateFlags();

    public abstract Date getUpdatedAt();

    public abstract Optional<Date> getRetryAt();

    public abstract Config getStateParams();

    public abstract Optional<TaskReport> getReport();

    public abstract Optional<Config> getError();  // TODO error class

    @Value.Check
    protected void check()
    {
        checkState(!getError().isPresent() || !getError().get().isEmpty(), "error must not be empty if not null");
    }
}
