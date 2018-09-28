package io.digdag.standards.operator.param;

import io.digdag.spi.ParamServerClientConnection;

public class DummyServerClientConnection
        implements ParamServerClientConnection
{
    @Override
    public Object get()
    {
        return null;
    }

    @Override
    public String getType()
    {
        return "dummy";
    }
}
