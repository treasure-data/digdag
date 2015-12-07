package io.digdag.spi;

import com.google.common.base.Optional;

public interface TaskQueue
{
    void put(TaskRequest request);

    Optional<TaskRequest> receive(long timeoutMillis)
            throws InterruptedException;  // TODO receiver node options
}
