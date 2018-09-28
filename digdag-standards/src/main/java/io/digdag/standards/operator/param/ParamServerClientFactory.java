package io.digdag.standards.operator.param;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.spi.ParamServerClient;
import io.digdag.spi.ParamServerClientConnection;

public class ParamServerClientFactory
{
    public static ParamServerClient build(ParamServerClientConnection connection, ObjectMapper objectMapper)
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
