package io.digdag.core;

import com.google.common.base.Optional;

public interface TaskQueue
{
    void put(Action action);

    Optional<Action> receive(long timeoutMillis)
            throws InterruptedException;  // TODO receiver node options
}
