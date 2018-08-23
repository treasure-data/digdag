package io.digdag.standards.operator.param;

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
