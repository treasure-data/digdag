package io.digdag.standards.operator.param;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.digdag.client.config.Config;
import io.digdag.core.database.DatabaseConfig;

import java.util.Arrays;
import java.util.List;

public class ParamServerDatabaseConfigProvider
        implements Provider<DatabaseConfig>
{
    private final DatabaseConfig config;
    private final List<String> supportedDatabases = Arrays.asList("postgresql");

    @Inject
    public ParamServerDatabaseConfigProvider(Config systemConfig)
    {
        String databaseType = systemConfig.get("param_server.database.type", String.class, "");
        if (supportedDatabases.contains(databaseType)) {
            this.config = DatabaseConfig.convertFrom(systemConfig, "param_server.database");
        }
        else {
            this.config = DatabaseConfig.createDummy();
        }
    }

    @Override
    public DatabaseConfig get()
    {
        return config;
    }
}
