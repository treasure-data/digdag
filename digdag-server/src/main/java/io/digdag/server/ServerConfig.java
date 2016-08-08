package io.digdag.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import org.immutables.value.Value;

import java.util.Map;

import static java.util.stream.Collectors.toMap;

@Value.Immutable
@JsonSerialize(as = ImmutableServerConfig.class)
@JsonDeserialize(as = ImmutableServerConfig.class)
public interface ServerConfig
{
    public static final int DEFAULT_PORT = 65432;
    public static final String DEFAULT_BIND = "127.0.0.1";
    public static final String DEFAULT_ACCESS_LOG_PATTERN = "json";

    static String HEADER_KEY_PREFIX = "server.http.headers.";

    public int getPort();

    public String getBind();

    public Optional<String> getServerRuntimeInfoPath();

    public Optional<String> getAccessLogPath();

    public String getAccessLogPattern();

    public boolean getExecutorEnabled();

    public Map<String, String> getHeaders();

    public ConfigElement getSystemConfig();

    public static ImmutableServerConfig.Builder defaultBuilder()
    {
        return ImmutableServerConfig.builder()
            .port(DEFAULT_PORT)
            .bind(DEFAULT_BIND)
            .accessLogPattern(DEFAULT_ACCESS_LOG_PATTERN)
            .executorEnabled(true);
    }

    public static ServerConfig defaultConfig()
    {
        return defaultBuilder().build();
    }

    public static ServerConfig convertFrom(Config config)
    {
        Map<String, String> headers = config.getKeys().stream()
                .filter(key -> key.startsWith(HEADER_KEY_PREFIX))
                .collect(toMap(key -> key.substring(HEADER_KEY_PREFIX.length()),
                        key -> config.get(key, String.class)));

        return defaultBuilder()
            .port(config.get("server.port", int.class, DEFAULT_PORT))
            .bind(config.get("server.bind", String.class, DEFAULT_BIND))
            .serverRuntimeInfoPath(config.getOptional("server.runtime-info.path", String.class))
            .accessLogPath(config.getOptional("server.access-log.path", String.class))
            .accessLogPattern(config.get("server.access-log.pattern", String.class, DEFAULT_ACCESS_LOG_PATTERN))
            .executorEnabled(config.get("server.executor.enabled", boolean.class, true))
            .headers(headers)
            .systemConfig(ConfigElement.copyOf(config))  // systemConfig needs to include other keys such as server.port so that ServerBootstrap.initialize can recover ServerConfig from this systemConfig
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
