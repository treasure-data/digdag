package io.digdag.core.queue;

import java.time.Instant;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableStoredQueueSetting.class)
@JsonDeserialize(as = ImmutableStoredQueueSetting.class)
public abstract class StoredQueueSetting
        extends QueueSetting
{
    public abstract long getId();

    public abstract int getSiteId();

    public abstract Instant getCreatedAt();

    public abstract Instant getUpdatedAt();
}
