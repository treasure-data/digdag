package io.digdag.standards.operator.param;

import com.google.inject.Inject;
import com.google.inject.Provider;

public class ParamServerClientProvider
        implements Provider<ParamServerClient>
{
    private final ParamServerClientConnection connection;

    @Inject
    public ParamServerClientProvider(ParamServerClientConnection connection)
    {
        this.connection = connection;
    }

    @Override
    public ParamServerClient get()
    {
        switch (connection.getType()) {
            case "postgresql":
                return new PostgresqlParamServerClient(connection);
            case "redis":
                return new RedisParamServerClient(connection);
            default:
                return new DummyParamServerClient(connection);
        }
    }
}
