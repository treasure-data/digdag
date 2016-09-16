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

    static ImmutableDatabaseConfig.Builder builder()
    {
        return ImmutableDatabaseConfig.builder();
    }

    static DatabaseConfig convertFrom(Config config)
    {
        ImmutableDatabaseConfig.Builder builder = builder();

        // database.type, path, host, user, password, port, database
        String type = config.get("database.type", String.class, "memory");
        switch (type) {
        case "h2":
            builder.type("h2");
            builder.path(Optional.of(config.get("database.path", String.class)));
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
                    .user(config.get("database.user", String.class))
                    .password(config.get("database.password", String.class, ""))
                    .host(config.get("database.host", String.class))
                    .port(config.getOptional("database.port", Integer.class))
                    .database(config.get("database.database", String.class))
                    .loginTimeout(config.get("database.loginTimeout", int.class, 30))
                    .socketTimeout(config.get("database.socketTimeout", int.class, 1800))
                    .ssl(config.get("database.ssl", boolean.class, false))
                    .build()));
            break;
        default:
            throw new ConfigException("Unknown database.type: " + type);
        }

        builder.connectionTimeout(
                config.get("database.connectionTimeout", int.class, 30));  // HikariCP default: 30
        builder.idleTimeout(
                config.get("database.idleTimeout", int.class, 600));  // HikariCP default: 600
        builder.validationTimeout(
                config.get("database.validationTimeout", int.class, 5));  // HikariCP default: 5

        int maximumPoolSize = config.get("database.maximumPoolSize", int.class,
                Runtime.getRuntime().availableProcessors() * 32); // HikariCP default: 10

        builder.maximumPoolSize(maximumPoolSize);
        builder.minimumPoolSize(
                 config.get("database.minimumPoolSize", int.class, maximumPoolSize));  // HikariCP default: Same as maximumPoolSize

        // database.opts.* to options
        ImmutableMap.Builder<String, String> options = ImmutableMap.builder();
        for (String key : config.getKeys()) {
            if (key.startsWith("database.opts.")) {
                options.put(key.substring("database.opts.".length()), config.get(key, String.class));
            }
        }
        builder.options(options.build());

        builder.autoMigrate(
                config.get("database.migrate", boolean.class, true));

        builder.expireLockInterval(
                config.get("database.queue.expireLockInterval", int.class, 10));

        return builder.build();
    }

    static Config toConfig(DatabaseConfig databaseConfig, ConfigFactory cf) {
        Config config = cf.create();

        config.set("database.type", databaseConfig.getType());
        switch (databaseConfig.getType()) {
            case "h2":
                config.setOptional("database.path", databaseConfig.getPath());
                break;
            case "memory":
                break;
            case "postgresql":
                RemoteDatabaseConfig remoteDatabaseConfig = databaseConfig.getRemoteDatabaseConfig().orNull();
                assert remoteDatabaseConfig != null;
                config.set("database.user", remoteDatabaseConfig.getUser());
                config.set("database.password", remoteDatabaseConfig.getPassword());
                config.set("database.host", remoteDatabaseConfig.getHost());
                config.setOptional("database.port", remoteDatabaseConfig.getPort());
                config.set("database.database", remoteDatabaseConfig.getDatabase());
                config.set("database.loginTimeout", remoteDatabaseConfig.getLoginTimeout());
                config.set("database.socketTimeout", remoteDatabaseConfig.getSocketTimeout());
                config.set("database.ssl", remoteDatabaseConfig.getSsl());
                break;
            default:
                throw new AssertionError("Unknown database.type: " + databaseConfig.getType());
        }

        config.set("database.connectionTimeout", databaseConfig.getConnectionTimeout());
        config.set("database.idleTimeout", databaseConfig.getIdleTimeout());
        config.set("database.validationTimeout", databaseConfig.getValidationTimeout());
        config.set("database.maximumPoolSize", databaseConfig.getMaximumPoolSize());
        config.set("database.minimumPoolSize", databaseConfig.getMinimumPoolSize());

        // database.opts.*
        Map<String, String> options = databaseConfig.getOptions();
        for (String key : options.keySet()) {
            config.set("database.opts." + key, options.get(key));
        }

        config.set("database.migrate", databaseConfig.getAutoMigrate());

        config.set("database.queue.expireLockInterval", databaseConfig.getExpireLockInterval());

        return config;
    }

    static String buildJdbcUrl(DatabaseConfig config)
    {
        switch (config.getType()) {
        case "h2":
            if (config.getRemoteDatabaseConfig().isPresent()) {
                throw new IllegalArgumentException("Database type is postgresql but remoteDatabaseConfig is not set unexpectedly");
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
                props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");  // disable server certificate validation
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
