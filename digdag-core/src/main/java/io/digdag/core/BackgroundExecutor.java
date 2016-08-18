package io.digdag.core;

public interface BackgroundExecutor
{
    void eagerShutdown() throws Exception;
}
