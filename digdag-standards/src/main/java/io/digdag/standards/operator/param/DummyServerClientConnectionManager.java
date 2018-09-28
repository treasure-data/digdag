package io.digdag.standards.operator.param;

import io.digdag.spi.ParamServerClientConnection;
import io.digdag.spi.ParamServerClientConnectionManager;

public class DummyServerClientConnectionManager
        implements ParamServerClientConnectionManager
{
    @Override
    public ParamServerClientConnection getConnection()
    {
        return new DummyServerClientConnection();
    }

    @Override
    public void shutdown()
    {

    }
}
