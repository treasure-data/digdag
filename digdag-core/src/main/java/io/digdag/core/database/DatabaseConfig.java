package io.digdag.core.database;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import org.immutables.value.Value;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Value.Immutable
public interface DatabaseConfig
{
    String getType();

    Optional<String> getPath();

    Map<String, String> getOptions();

    Optional<RemoteDatabaseConfig> getRemoteDatabaseConfig();

    int getExpireLockInterval();

    boolean getAutoMigrate();

    ////
    // HikariCP config params
    //

    int getConnectionTimeout();  // seconds

    int getIdleTimeout();  // seconds

    int getMaximumPoolSize();

    int getMinimumPoolSize();

    int getValidationTimeout();  // seconds

    boolean getEnableJMX();

    long getLeakDetectionThreshold();  // milliseconds

    static ImmutableDatabaseConfig.Builder builder()
    {
        return ImmutableDatabaseConfig.builder();
    }

    static DatabaseConfig convertFrom(Config config)
    {
        return convertFrom(config, "database");
    }

    static DatabaseConfig convertFrom(Config config, String keyPrefix)
    {
        ImmutableDatabaseConfig.Builder builder = builder();

        // database.type, path, host, user, password, port, database
        String type = config.get(keyPrefix + "." + "type", String.class, "memory");
        switch (type) {
        case "h2":
            builder.type("h2");
            builder.path(Optional.of(config.get(keyPrefix + "." + "path", String.class)));
            builder.remoteDatabaseConfig(Optional.absent());
            break;
        case "memory":
            builder.type("h2");
            builder.path(Optional.absent());
            builder.remoteDatabaseConfig(Optional.absent());
            break;
        case "postgresql":
            builder.type("postgresql");
            builder.remoteDatabaseConfig(Optional.of(
                RemoteDatabaseConfig.builder()
                    .user(config.get(keyPrefix + "." + "user", String.class))
                    .password(config.get(keyPrefix + "." + "password", String.class, ""))
                    .host(config.get(keyPrefix + "." + "host", String.class))
                    .port(config.getOptional(keyPrefix + "." + "port", Integer.class))
                    .database(config.get(keyPrefix + "." + "database", String.class))
                    .loginTimeout(config.get(keyPrefix + "." + "loginTimeout", int.class, 30))
                    .socketTimeout(config.get(keyPrefix + "." + "socketTimeout", int.class, 1800))
                    .ssl(config.get(keyPrefix + "." + "ssl", boolean.class, false))
                    .sslfactory(config.get(keyPrefix + "." + "sslfactory", String.class, "org.postgresql.ssl.NonValidatingFactory"))
                    .sslmode(config.getOptional(keyPrefix + "." + "sslmode", String.class))
                    .build()));
            break;
        default:
            throw new ConfigException("Unknown database.type: " + type);
        }

        builder.connectionTimeout(
                config.get(keyPrefix + "." + "connectionTimeout", int.class, 30));  // HikariCP default: 30
        builder.idleTimeout(
                config.get(keyPrefix + "." + "idleTimeout", int.class, 600));  // HikariCP default: 600
        builder.validationTimeout(
                config.get(keyPrefix + "." + "validationTimeout", int.class, 5));  // HikariCP default: 5

        int maximumPoolSize = config.get(keyPrefix + "." + "maximumPoolSize", int.class,
                Runtime.getRuntime().availableProcessors() * 32); // HikariCP default: 10

        builder.maximumPoolSize(maximumPoolSize);
        builder.minimumPoolSize(
                config.get(keyPrefix + "." + "minimumPoolSize", int.class, maximumPoolSize));  // HikariCP default: Same as maximumPoolSize

        builder.enableJMX(isJMXEnable(config));
        builder.leakDetectionThreshold(
                config.get(keyPrefix + "." + "leakDetectionThreshold", long.class, 0L));  // HikariCP default: 0

        // database.opts.* to options
        ImmutableMap.Builder<String, String> options = ImmutableMap.builder();
        for (String key : config.getKeys()) {
            String optionKey = keyPrefix + "." + "opts.";
            if (key.startsWith(optionKey)) {
                options.put(key.substring(optionKey.length()), config.get(key, String.class));
            }
        }
        builder.options(options.build());

        builder.autoMigrate(
                config.get(keyPrefix + "." + "migrate", boolean.class, true));

        builder.expireLockInterval(
                config.get(keyPrefix + "." + "queue.expireLockInterval", int.class, 10));

        return builder.build();
    }

    /**
     * If server.jmx.port exists, then JMX is enable
     * TODO this method should move to proper class?
     * @param config
     * @return
     */
    static boolean isJMXEnable(Config config)
    {
        return config.getOptional("server.jmx.port", Integer.class)
                .transform((port) -> true)
                .or(false);
    }

    static Config toConfig(DatabaseConfig databaseConfig, ConfigFactory cf) {
        return toConfig(databaseConfig, cf, "database");
    }

    static Config toConfig(DatabaseConfig databaseConfig, ConfigFactory cf, String keyPrefix) {
        Config config = cf.create();

        config.set(keyPrefix + "." + "type", databaseConfig.getType());
        switch (databaseConfig.getType()) {
            case "h2":
                config.setOptional(keyPrefix + "." + "path", databaseConfig.getPath());
                break;
            case "memory":
                break;
            case "postgresql":
                RemoteDatabaseConfig remoteDatabaseConfig = databaseConfig.getRemoteDatabaseConfig().orNull();
                assert remoteDatabaseConfig != null;
                config.set(keyPrefix + "." + "user", remoteDatabaseConfig.getUser());
                config.set(keyPrefix + "." + "password", remoteDatabaseConfig.getPassword());
                config.set(keyPrefix + "." + "host", remoteDatabaseConfig.getHost());
                config.setOptional(keyPrefix + "." + "port", remoteDatabaseConfig.getPort());
                config.set(keyPrefix + "." + "database", remoteDatabaseConfig.getDatabase());
                config.set(keyPrefix + "." + "loginTimeout", remoteDatabaseConfig.getLoginTimeout());
                config.set(keyPrefix + "." + "socketTimeout", remoteDatabaseConfig.getSocketTimeout());
                config.set(keyPrefix + "." + "ssl", remoteDatabaseConfig.getSsl());
                config.set(keyPrefix + "." + "sslfactory", remoteDatabaseConfig.getSslfactory());
                config.setOptional(keyPrefix + "." + "sslmode", remoteDatabaseConfig.getSslmode());
                break;
            default:
                throw new AssertionError("Unknown database.type: " + databaseConfig.getType());
        }

        config.set(keyPrefix + "." + "connectionTimeout", databaseConfig.getConnectionTimeout());
        config.set(keyPrefix + "." + "idleTimeout", databaseConfig.getIdleTimeout());
        config.set(keyPrefix + "." + "validationTimeout", databaseConfig.getValidationTimeout());
        config.set(keyPrefix + "." + "maximumPoolSize", databaseConfig.getMaximumPoolSize());
        config.set(keyPrefix + "." + "minimumPoolSize", databaseConfig.getMinimumPoolSize());
        config.set(keyPrefix + "." + "enableJMX", databaseConfig.getEnableJMX());

        // database.opts.*
        Map<String, String> options = databaseConfig.getOptions();
        for (String key : options.keySet()) {
            config.set(keyPrefix + "." + "opts." + key, options.get(key));
        }

        config.set(keyPrefix + "." + "migrate", databaseConfig.getAutoMigrate());

        config.set(keyPrefix + "." + "queue.expireLockInterval", databaseConfig.getExpireLockInterval());

        return config;
    }

    static String buildJdbcUrl(DatabaseConfig config)
    {
        switch (config.getType()) {
        case "h2":
            if (config.getRemoteDatabaseConfig().isPresent()) {
                throw new IllegalArgumentException("Database type is h2 but remoteDatabaseConfig is not set unexpectedly");
            }
            if (config.getPath().isPresent()) {
                Path dir = FileSystems.getDefault().getPath(config.getPath().get());
                try {
                    Files.createDirectories(dir);
                }
                catch (IOException ex) {
                    throw new ConfigException(ex);
                }
                return String.format(Locale.ENGLISH,
                        "jdbc:h2:%s",
                        dir.resolve("digdag").toAbsolutePath().toString());  // h2 requires absolute path
            }
            else {
                return String.format(Locale.ENGLISH,
                        "jdbc:h2:mem:digdag-%s",
                        UUID.randomUUID());
            }

        case "postgresql":
            {
                if (!config.getRemoteDatabaseConfig().isPresent()) {
                    throw new IllegalArgumentException("Database type is postgresql but remoteDatabaseConfig is not set unexpectedly");
                }
                RemoteDatabaseConfig remote = config.getRemoteDatabaseConfig().get();
                if (remote.getPort().isPresent()) {
                    return String.format(Locale.ENGLISH,
                            "jdbc:postgresql://%s:%d/%s",
                            remote.getHost(), remote.getPort().get(), remote.getDatabase());
                }
                else {
                    return String.format(Locale.ENGLISH,
                            "jdbc:postgresql://%s/%s",
                            remote.getHost(), remote.getDatabase());
                }
            }

        default:
            throw new ConfigException("Unsupported database type: "+config.getType());
        }
    }

    static Properties buildJdbcProperties(DatabaseConfig config)
    {
        Properties props = new Properties();
        Optional<RemoteDatabaseConfig> rc = config.getRemoteDatabaseConfig();

        // add default params
        switch (config.getType()) {
        case "h2":
            // nothing
            break;

        case "postgresql":
            props.setProperty("loginTimeout", Integer.toString(rc.get().getLoginTimeout())); // seconds
            props.setProperty("socketTimeout", Integer.toString(rc.get().getSocketTimeout())); // seconds
            props.setProperty("tcpKeepAlive", "true");
            break;

        default:
            throw new ConfigException("Unsupported database type: "+config.getType());
        }

        if (config.getRemoteDatabaseConfig().isPresent()) {
            props.setProperty("user", rc.get().getUser());
            props.setProperty("password", rc.get().getPassword());
            if (rc.get().getSsl()) {
                props.setProperty("ssl", "true");
                props.setProperty("sslfactory", rc.get().getSslfactory());
                if (rc.get().getSslmode().isPresent()) {
                    props.setProperty("sslmode", rc.get().getSslmode().get());
                }
            }
        }

        for (Map.Entry<String, String> pair : config.getOptions().entrySet()) {
            props.setProperty(pair.getKey(), pair.getValue());
        }

        return props;
    }

    static boolean isPostgres(String databaseType)
    {
        return databaseType.equals("postgresql");
    }
}
