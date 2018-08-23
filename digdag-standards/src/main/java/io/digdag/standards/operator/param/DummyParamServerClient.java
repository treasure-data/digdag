package io.digdag.standards.operator.param;

import com.google.common.base.Optional;

import java.util.function.Consumer;

public class DummyParamServerClient
        implements ParamServerClient
{
    public DummyParamServerClient(ParamServerClientConnection _) {

    }

    @Override
    public Optional<String> get(String key, int sitedId)
    {
        return null;
    }

    @Override
    public void set(String key, String value, int siteId)
    {

    }

    @Override
    public void doTransaction(Consumer<ParamServerClient> consumer)
    {

    }

    @Override
    public void commit()
    {

    }
}
