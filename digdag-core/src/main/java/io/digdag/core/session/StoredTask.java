package io.digdag.core.session;

import java.util.List;
import java.time.Instant;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.spi.TaskReport;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableStoredTask.class)
public abstract class StoredTask
        extends Task
{
    public abstract long getId();

    public abstract long getAttemptId();

    public abstract List<Long> getUpstreams();  // list of task_id

    public abstract TaskStateFlags getStateFlags();

    public abstract Instant getUpdatedAt();

    public abstract Optional<Instant> getRetryAt();

    public abstract Config getStateParams();
}
