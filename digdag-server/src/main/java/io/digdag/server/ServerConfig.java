package io.digdag.server;

import java.util.List;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.api.RestApiKey;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableServerConfig.class)
@JsonDeserialize(as = ImmutableServerConfig.class)
public abstract class ServerConfig
{
    public static final int DEFAULT_PORT = 65432;
    public static final String DEFAULT_BIND = "127.0.0.1";

    public abstract int getPort();

    public abstract String getBind();

    public abstract Optional<String> getAutoLoadLocalDagfile();

    public abstract boolean getAllowPublicAccess();

    public abstract List<UserConfig> getApiKeyAuthUsers();

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
        Optional<RestApiKey> apikey = config.getOptional("server.apikey", RestApiKey.class);
        List<UserConfig> users = apikey.transform(key -> ImmutableList.<UserConfig>of(
                UserConfig.builder()
                    .siteId(0)
                    .apiKey(key)
                    .build()
                )).or(ImmutableList.of());
        return defaultBuilder()
            .port(config.get("server.port", int.class, DEFAULT_PORT))
            .bind(config.get("server.bind", String.class, DEFAULT_BIND))
            .autoLoadLocalDagfile(config.getOptional("server.autoLoadLocalDagfile", String.class))
            .allowPublicAccess(users.isEmpty())
            .apiKeyAuthUsers(users)
            .build();
    }

    public static ServerConfig convertFrom(ConfigElement configElement)
    {
        ConfigFactory cf = new ConfigFactory(
                new ObjectMapper()
                .registerModule(new GuavaModule())
                );
        return convertFrom(configElement.toConfig(cf));
    }
}
