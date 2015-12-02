package io.digdag.core.spi;

import com.google.common.base.Optional;
import io.digdag.core.spi.TaskRequest;

public interface TaskQueue
{
    void put(TaskRequest request);

    Optional<TaskRequest> receive(long timeoutMillis)
            throws InterruptedException;  // TODO receiver node options
}
