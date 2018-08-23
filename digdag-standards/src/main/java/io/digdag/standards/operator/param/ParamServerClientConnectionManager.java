package io.digdag.standards.operator.param;

public interface ParamServerClientConnectionManager
{
    ParamServerClientConnection getConnection();

    void shutdown();
}
