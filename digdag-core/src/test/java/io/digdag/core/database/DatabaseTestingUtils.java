package io.digdag.core.database;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigFactory;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;

public class DatabaseTestingUtils
{
    private DatabaseTestingUtils() { }

    public static DbiProvider setupDatabase()
    {
        DatabaseStoreConfig config = DatabaseStoreConfig.builder()
            .type("h2")
            .url("jdbc:h2:mem:test" + System.nanoTime())
            .build();
        PooledDataSourceProvider dsp = new PooledDataSourceProvider(config);
        IDBI dbi = new DBI(dsp.get());
        new DatabaseMigrator(dbi, "h2").migrate();
        return new DbiProvider(dbi, dsp);
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
