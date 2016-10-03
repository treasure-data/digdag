package io.digdag.core;

import io.digdag.guice.rs.server.PreStop;

public interface BackgroundExecutor
{
    @PreStop
    void eagerShutdown() throws Exception;
}
