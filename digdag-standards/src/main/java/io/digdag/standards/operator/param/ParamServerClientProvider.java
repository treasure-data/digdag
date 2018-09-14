package io.digdag.standards.operator.param;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class ParamServerClientProvider
        implements Provider<ParamServerClient>
{
    private final ParamServerClientConnection connection;
    private final ObjectMapper objectMapper;

    @Inject
    public ParamServerClientProvider(ParamServerClientConnection connection, ObjectMapper objectMapper)
    {
        this.connection = connection;
        this.objectMapper = objectMapper;
    }

    @Override
    public ParamServerClient get()
    {
        switch (connection.getType()) {
            case "postgresql":
                return new PostgresqlParamServerClient(connection, objectMapper);
            case "redis":
                return new RedisParamServerClient(connection, objectMapper);
            default:
                return new DummyParamServerClient(connection, objectMapper);
        }
    }
}
