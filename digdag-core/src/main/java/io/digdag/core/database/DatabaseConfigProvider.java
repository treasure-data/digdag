package io.digdag.core.database;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;

public class DatabaseConfigProvider
    implements Provider<DatabaseConfig>
{
    private final DatabaseConfig config;

    @Inject
    public DatabaseConfigProvider(ConfigElement ce, ConfigFactory cf)
    {
        this.config = DatabaseConfig.convertFrom(ce.toConfig(cf));
    }

    @Override
    public DatabaseConfig get()
    {
        return config;
    }
}
