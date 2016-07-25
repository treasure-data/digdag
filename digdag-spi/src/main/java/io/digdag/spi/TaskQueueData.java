package io.digdag.spi;

import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public interface TaskQueueData
{
    Optional<Long> getUniqueTaskId();

    Optional<byte[]> getData();
}
