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
    @Inject
    public DatabaseConfigProvider(Optional<ConfigElement> ce, ConfigFactory cf)
    {
        Config config = ce.transform(e -> e.toConfig(cf)).or(cf.create());
    }

    @Override
    public DatabaseConfig get()
    {
        return DatabaseConfig.builder()
            .build();
    }
}
