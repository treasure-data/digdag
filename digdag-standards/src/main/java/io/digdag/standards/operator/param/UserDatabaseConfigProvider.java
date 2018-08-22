package io.digdag.standards.operator.param;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.core.database.DatabaseConfig;

public class UserDatabaseConfigProvider
        implements Provider<DatabaseConfig>
{
    private final DatabaseConfig config;

    @Inject
    public UserDatabaseConfigProvider(Config systemConfig)
    {
        this.config = DatabaseConfig.convertFrom(systemConfig, "param_server.database");
    }

    @Override
    public DatabaseConfig get()
    {
        return config;
    }
}
