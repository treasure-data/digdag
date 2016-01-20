package io.digdag.core.schedule;

import java.util.Date;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredSchedule.class)
@JsonDeserialize(as = ImmutableStoredSchedule.class)
public abstract class StoredSchedule
        extends Schedule
{
    public abstract long getId();

    public abstract Config getConfig();

    public abstract Date getCreatedAt();

    public abstract Date getUpdatedAt();
}
