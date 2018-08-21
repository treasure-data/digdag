package io.digdag.core.database;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;

public class UserDatabaseConfigProvider
        implements Provider<DatabaseConfig>
{
    private final DatabaseConfig config;

    @Inject
    public UserDatabaseConfigProvider(Config systemConfig)
    {
        this.config = DatabaseConfig.convertFrom(systemConfig, "user_database");
    }

    @Override
    public DatabaseConfig get()
    {
        return config;
    }
}
