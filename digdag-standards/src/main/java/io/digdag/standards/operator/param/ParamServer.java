package io.digdag.standards.operator.param;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import org.skife.jdbi.v2.DBI;

public class ParamServer
{
    public static ParamServerClient getClient(String type, Config sysConfig, DBI dbi)
    {
        switch (type) {
            case "redis":
                return new RedisServerClient(sysConfig);
            case "postgresql":
                return new PostgresqlServerClient(sysConfig, dbi);
            default:
                throw new ConfigException("Not supported database type: " + type);
        }
    }
}
