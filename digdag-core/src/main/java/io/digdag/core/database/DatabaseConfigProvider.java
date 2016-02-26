package io.digdag.core.database;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;

public class DatabaseConfigProvider
    implements Provider<DatabaseConfig>
{
    private final DatabaseConfig config;

    @Inject
    public DatabaseConfigProvider(Config systemConfig)
    {
        this.config = DatabaseConfig.convertFrom(systemConfig);
    }

    @Override
    public DatabaseConfig get()
    {
        return config;
    }
}
