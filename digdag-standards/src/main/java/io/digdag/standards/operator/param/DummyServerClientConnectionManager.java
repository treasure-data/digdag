package io.digdag.standards.operator.param;

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
