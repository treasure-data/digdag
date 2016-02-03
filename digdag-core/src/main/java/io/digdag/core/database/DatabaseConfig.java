package io.digdag.core.database;

import java.util.Locale;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.FileSystems;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

@Value.Immutable
public abstract class DatabaseConfig
{
    public abstract String getType();

    public abstract Optional<String> getPath();

    public static ImmutableDatabaseConfig.Builder builder()
    {
        return ImmutableDatabaseConfig.builder();
    }

    public static DatabaseConfig convertFrom(Config config)
    {
        Config db = config.getNestedOrGetEmpty("database");
        return DatabaseConfig.builder()
            .type(db.get("type", String.class, "h2"))
            .path(db.getOptional("path", String.class))
            .build();
    }

    public static String buildJdbcUrl(DatabaseConfig config)
    {
        if (config.getType().equals("h2")) {
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
        }
        else {
            throw new ConfigException("Unsupported database type: "+config.getType());
        }
    }
}
