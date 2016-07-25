package io.digdag.spi;

import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@Value.Immutable
@JsonDeserialize(as = ImmutableTaskQueueLock.class)
public interface TaskQueueLock
    extends TaskQueueData
{
    String getLockId();

    static ImmutableTaskQueueLock.Builder builder()
    {
        return ImmutableTaskQueueLock.builder();
    }
}
