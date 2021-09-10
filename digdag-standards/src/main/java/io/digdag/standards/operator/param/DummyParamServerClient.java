package io.digdag.standards.operator.param;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.spi.ParamServerClient;
import io.digdag.spi.ParamServerClientConnection;
import io.digdag.spi.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class DummyParamServerClient
        implements ParamServerClient
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    public DummyParamServerClient(ParamServerClientConnection _connection, ObjectMapper _objectMapper)
    {

    }

    @Override
    public Optional<Record> get(String key, int sitedId)
    {
        logger.warn("This is a Dummy Implementation, so just will return null");
        return Optional.absent();
    }

    @Override
    public void set(String key, String value, int siteId)
    {
        logger.warn("This is a Dummy Implementation, so no value is set");
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
