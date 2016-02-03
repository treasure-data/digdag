package io.digdag.server;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.immutables.value.Value;

@Value.Immutable
public abstract class ServerConfig
{
    public static final int DEFAULT_PORT = 65432;
    public static final String DEFAULT_BIND = "127.0.0.1";

    public abstract int getPort();

    public abstract String getBind();

    public abstract Optional<String> getAutoLoadLocalDagfile();

    public static ImmutableServerConfig.Builder builder()
    {
        return ImmutableServerConfig.builder();
    }

    private static ImmutableServerConfig.Builder defaultBuilder()
    {
        return builder()
            .port(DEFAULT_PORT)
            .bind(DEFAULT_BIND);
    }

    public static ServerConfig defaultConfig()
    {
        return defaultBuilder().build();
    }

    public static ServerConfig convertFrom(Config config)
    {
        Config sv = config.getNestedOrGetEmpty("server");
        return defaultBuilder()
            .port(sv.get("port", int.class, DEFAULT_PORT))
            .bind(sv.get("bind", String.class, DEFAULT_BIND))
            .build();
    }

    public static ServerConfig convertFrom(ConfigElement configElement)
    {
        ConfigFactory cf = new ConfigFactory(new ObjectMapper());
        return convertFrom(configElement.toConfig(cf));
    }
}
