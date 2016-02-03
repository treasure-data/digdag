package io.digdag.server;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;

public class ServerConfigProvider
    implements Provider<ServerConfig>
{
    @Inject
    public ServerConfigProvider(ConfigElement ce, ConfigFactory cf)
    {
        //Config config = ce.transform(e -> e.toConfig(cf)).or(cf.create());
    }

    @Override
    public ServerConfig get()
    {
        return ServerConfig.builder()
            .build();
    }
}
