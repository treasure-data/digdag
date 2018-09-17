package io.digdag.standards.operator.param;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.spi.ParamServerClient;
import io.digdag.spi.ParamServerClientConnection;
import io.digdag.spi.Record;

import java.util.function.Consumer;

public class DummyParamServerClient
        implements ParamServerClient
{
    public DummyParamServerClient(ParamServerClientConnection _connection, ObjectMapper _objectMapper)
    {

    }

    @Override
    public Optional<Record> get(String key, int sitedId)
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
