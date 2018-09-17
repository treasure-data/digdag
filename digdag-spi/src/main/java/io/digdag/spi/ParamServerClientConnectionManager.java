package io.digdag.spi;

public interface ParamServerClientConnectionManager
{
    ParamServerClientConnection getConnection();

    void shutdown();
}
