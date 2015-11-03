package io.digdag.core.spi;

import com.google.common.base.Optional;
import io.digdag.core.queue.Action;

public interface TaskQueue
{
    void put(Action action);

    Optional<Action> receive(long timeoutMillis)
            throws InterruptedException;  // TODO receiver node options
}
