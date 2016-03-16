package io.digdag.core.database;

import java.util.Map;
import java.util.Properties;
import java.util.Locale;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

@Value.Immutable
public interface DatabaseConfig
{
    String getType();

    Optional<String> getPath();

    Map<String, String> getOptions();

    Optional<RemoteDatabaseConfig> getRemoteDatabaseConfig();

    int getExpireLockInterval();

    boolean getAutoMigrate();

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
                    .build()));
            break;
        default:
            throw new ConfigException("Unknown database.type: " + type);
        }

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

    static String buildJdbcUrl(DatabaseConfig config)
    {
        switch (config.getType()) {
        case "h2":
            // DB should be closed by @PreDestroy otherwise DB could be closed before other @PreDestroy methods that access to the DB
            if (config.getPath().isPresent()) {
                Path dir = FileSystems.getDefault().getPath(config.getPath().get());
                try {
                    Files.createDirectories(dir);
                }
                catch (IOException ex) {
                    throw new ConfigException(ex);
                }
                return String.format(Locale.ENGLISH,
                        "jdbc:h2:%s;DB_CLOSE_ON_EXIT=FALSE",
                        dir.resolve("digdag").toAbsolutePath().toString());  // h2 requires absolute path
            }
            else {
                return String.format(Locale.ENGLISH,
                        "jdbc:h2:mem:digdag;DB_CLOSE_ON_EXIT=FALSE");
            }

        case "postgresql":
            {
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
        for (Map.Entry<String, String> pair : config.getOptions().entrySet()) {
            props.setProperty(pair.getKey(), pair.getValue());
        }

        // add default params
        switch (config.getType()) {
        case "h2":
            // nothing
            break;
        case "postgresql":
            props.setProperty("loginTimeout",   "300"); // seconds
            props.setProperty("socketTimeout", "1800"); // seconds
            props.setProperty("tcpKeepAlive", "true");
            break;
        }

        if (config.getRemoteDatabaseConfig().isPresent()) {
            props.setProperty("user", config.getRemoteDatabaseConfig().get().getUser());
            props.setProperty("password", config.getRemoteDatabaseConfig().get().getPassword());
        }
        return props;
    }
}
