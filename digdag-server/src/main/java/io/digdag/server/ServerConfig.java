package io.digdag.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.guice.rs.server.undertow.UndertowServerConfig;
import io.digdag.guice.rs.server.undertow.UndertowListenAddress;
import io.digdag.server.metrics.DigdagMetricsConfig;
import io.digdag.server.rs.AdminRestricted;
import org.immutables.value.Value;

import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

@Value.Immutable
@JsonDeserialize(as = ImmutableServerConfig.class)
public interface ServerConfig
    extends UndertowServerConfig
{
    public static final int DEFAULT_PORT = 65432;
    public static final String DEFAULT_BIND = "127.0.0.1";
    public static final String DEFAULT_ACCESS_LOG_PATTERN = "json";
    public static final String DEFAULT_AUTHENTICATOR_CLASS = "io.digdag.standards.auth.jwt.JwtAuthenticator";

    public Optional<String> getServerRuntimeInfoPath();

    public int getPort();

    public String getBind();

    public Optional<Integer> getAdminPort();

    public Optional<String> getAdminBind();

    public boolean getExecutorEnabled();

    public boolean getEnableSwagger();

    public Map<String, String> getHeaders();

    public ConfigElement getSystemConfig();

    public Map<String,String> getEnvironment();

    public String getAuthenticatorClass();

    public static ImmutableServerConfig.Builder defaultBuilder()
    {
        return ImmutableServerConfig.builder()
            .port(DEFAULT_PORT)
            .bind(DEFAULT_BIND)
            .accessLogPattern(DEFAULT_ACCESS_LOG_PATTERN)
            .authenticatorClass(DEFAULT_AUTHENTICATOR_CLASS)
            .executorEnabled(true);
    }

    public static ServerConfig defaultConfig()
    {
        return defaultBuilder().build();
    }

    public static ServerConfig convertFrom(Config config)
    {
        Function<String, Map<String, String>> readPrefixed = (prefix) -> config.getKeys().stream()
                .filter(key -> key.startsWith(prefix))
                .collect(toMap(key -> key.substring(prefix.length()),
                        key -> config.get(key, String.class)));

        return defaultBuilder()
            .port(config.get("server.port", int.class, DEFAULT_PORT))
            .bind(config.get("server.bind", String.class, DEFAULT_BIND))
            .adminPort(config.getOptional("server.admin.port", int.class))
            .adminBind(config.getOptional("server.admin.bind", String.class))
            .serverRuntimeInfoPath(config.getOptional("server.runtime-info.path", String.class))
            .accessLogPath(config.getOptional("server.access-log.path", String.class))
            .accessLogPattern(config.get("server.access-log.pattern", String.class, DEFAULT_ACCESS_LOG_PATTERN))
            .httpIoThreads(config.getOptional("server.http.io-threads", Integer.class))
            .httpWorkerThreads(config.getOptional("server.http.worker-threads", Integer.class))
            .httpNoRequestTimeout(config.getOptional("server.http.no-request-timeout", Integer.class))
            .httpRequestParseTimeout(config.getOptional("server.http.request-parse-timeout", Integer.class))
            .httpIoIdleTimeout(config.getOptional("server.http.io-idle-timeout", Integer.class))
            .jmxPort(config.getOptional("server.jmx.port", Integer.class))
            .enableHttp2(config.get("server.http.enable-http2", boolean.class, false))
            .executorEnabled(config.get("server.executor.enabled", boolean.class, true))
            .enableSwagger(config.get("server.enable-swagger", boolean.class, false))
            .headers(readPrefixed.apply("server.http.headers."))
            .systemConfig(ConfigElement.copyOf(config))  // systemConfig needs to include other keys such as server.port so that ServerBootstrap.initialize can recover ServerConfig from this systemConfig
            .environment(readPrefixed.apply("server.environment."))
            .authenticatorClass(config.get("server.authenticator-class", String.class, DEFAULT_AUTHENTICATOR_CLASS))
            .build();
    }

    public static ServerConfig convertFrom(ConfigElement configElement)
    {
        //ToDo replaced with configFactory()
        ConfigFactory cf = new ConfigFactory(
                new ObjectMapper()
                .registerModule(new GuavaModule())
                );
        return convertFrom(configElement.toConfig(cf));
    }

    public static ConfigFactory configFactory()
    {
        return new ConfigFactory(DigdagClient.objectMapper());
    }

    Set<Integer> getAdminSites();

    static final String API_ADDRESS = "api";

    static final String ADMIN_ADDRESS = "admin";

    default List<ListenAddress> getListenAddresses()
    {
        ImmutableList.Builder<ListenAddress> builder = ImmutableList.builder();
        builder.add(
                UndertowListenAddress.builder()
                .bind(getBind())
                .port(getPort())
                .name(API_ADDRESS)
                .build());
        if (getAdminPort().isPresent()) {
            builder.add(
                    UndertowListenAddress.builder()
                    .bind(getAdminBind().or(getBind()))
                    .port(getAdminPort().get())
                    .name(ADMIN_ADDRESS)
                    .build());
        }
        return builder.build();
    }
}
