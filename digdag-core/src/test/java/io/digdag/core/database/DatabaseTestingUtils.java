package io.digdag.core.database;

import com.google.common.collect.ImmutableMap;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import org.skife.jdbi.v2.DBI;

public class DatabaseTestingUtils
{
    private DatabaseTestingUtils() { }

    public static DatabaseFactory setupDatabase()
    {
        DatabaseConfig config = DatabaseConfig.builder()
            .type("h2")
            .path(Optional.absent())
            .remoteDatabaseConfig(Optional.absent())
            .options(ImmutableMap.of())
            .expireLockInterval(10)
            .autoMigrate(true)
            .connectionTimeout(30)
            .idleTimeout(600)
            .validationTimeout(5)
            .maximumPoolSize(10)
            .build();
        PooledDataSourceProvider dsp = new PooledDataSourceProvider(config);
        DBI dbi = new DBI(dsp.get());
        new DatabaseMigrator(dbi, config).migrate();
        return new DatabaseFactory(dbi, dsp, config);
    }

    public static ObjectMapper createObjectMapper()
    {
        return new ObjectMapper();
    }

    public static ConfigFactory createConfigFactory()
    {
        return new ConfigFactory(createObjectMapper());
    }

    public static ConfigMapper createConfigMapper()
    {
        return new ConfigMapper(createConfigFactory());
    }

    public static Config createConfig()
    {
        return createConfigFactory().create();
    }
}
