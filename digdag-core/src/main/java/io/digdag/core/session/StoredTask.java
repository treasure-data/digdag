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
    // Note that this ArchivedTask (which extends StoredTask) is serialized
    // and stored in the database (task_archives.tasks column). If you add a
    // new column, old attempts don't have the column stored. These fields will
    // be filled with a default value (0, null, or Optional.absent) by using
    // FAIL_ON_UNKNOWN_PROPERTIES=false option of ObjectMapper. See
    // DatabaseSessionStoreManager.loadTaskArchive for implementation.

    public abstract long getId();

    public abstract long getAttemptId();

    public abstract List<Long> getUpstreams();  // list of task_id

    public abstract Instant getUpdatedAt();

    public abstract Optional<Instant> getRetryAt();

    public abstract Optional<Instant> getStartedAt();

    public abstract Config getStateParams();

    @Value.Default
    public int getRetryCount()
    {
        return 0;
    }
}
