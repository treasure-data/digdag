package io.digdag.core.database;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.io.IOException;
import java.io.StringReader;
import java.time.ZoneId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.server.DigdagEmbed;
import io.digdag.core.repository.*;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import static io.digdag.client.DigdagClient.objectMapper;

public class DatabaseTestingUtils
{
    private DatabaseTestingUtils() { }

    public static DatabaseConfig getEnvironmentDatabaseConfig()
    {
        String pg = System.getenv("DIGDAG_TEST_POSTGRESQL");
        if (pg != null && !pg.isEmpty()) {
            Properties props = new Properties();
            try (StringReader reader = new StringReader(pg)) {
                props.load(reader);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }

            Config config = createConfig();
            for (String key : props.stringPropertyNames()) {
                config.set("database." + key, props.getProperty(key));
            }
            config.set("database.type", "postgresql");

            return DatabaseConfig.convertFrom(config);
        }
        else {
            return DatabaseConfig.builder()
                .type("h2")
                .path(Optional.absent())
                .remoteDatabaseConfig(Optional.absent())
                .options(ImmutableMap.of())
                .expireLockInterval(10)
                .autoMigrate(true)
                .connectionTimeout(30)
                .idleTimeout(600)
                .validationTimeout(5)
                .minimumPoolSize(0)
                .maximumPoolSize(10)
                .enableJMX(false)
                .leakDetectionThreshold(0)
                .build();
        }
    }

    public static DatabaseFactory setupDatabase()
    {
        return setupDatabase(false);
    }

    public static DatabaseFactory setupDatabase(boolean autoAutoCommit)
    {
        DatabaseConfig config = getEnvironmentDatabaseConfig();
        DataSourceProvider dsp = new DataSourceProvider(config);

        DBI dbi = new DBI(dsp.get());
        TransactionManager tm = new ThreadLocalTransactionManager(dsp.get(), autoAutoCommit);
        // FIXME
        new DatabaseMigrator(dbi, config).migrate();

        cleanDatabase(config.getType(), dbi);

        return new DatabaseFactory(tm, dsp, config);
    }

    public static final String[] ALL_TABLES = new String[] {
        "projects",
        "revisions",
        "revision_archives",
        "workflow_configs",
        "workflow_definitions",
        "schedules",
        "sessions",
        "session_attempts",
        "task_archives",
        "session_monitors",
        "task_dependencies",
        "queue_settings",
        "queues",
        "queued_tasks",
        "queued_task_locks",
    };

    public static void cleanDatabase(DigdagEmbed embed)
    {
        cleanDatabase(
                embed.getInjector().getInstance(DatabaseConfig.class).getType(),
                embed.getInjector().getInstance(DBI.class));
    }

    public static void cleanDatabase(String databaseType, IDBI dbi)
    {
        try (Handle handle = dbi.open()) {
            switch (databaseType) {
            case "h2":
                // h2 database can't truncate tables with references if REFERENTIAL_INTEGRITY is true (default)
                handle.createStatement("SET REFERENTIAL_INTEGRITY FALSE").execute();
                for (String name : Lists.reverse(Arrays.asList(ALL_TABLES))) {
                    handle.createStatement("TRUNCATE TABLE " + name).execute();
                }
                handle.createStatement("SET REFERENTIAL_INTEGRITY TRUE").execute();
                break;
            default:
                // postgresql needs "CASCADE" option to TRUNCATE
                for (String name : Lists.reverse(Arrays.asList(ALL_TABLES))) {
                    handle.createStatement("TRUNCATE " + name + " CASCADE").execute();
                }
                break;
            }
        }
    }

    public static ConfigFactory createConfigFactory()
    {
        return new ConfigFactory(objectMapper());
    }

    public static ConfigMapper createConfigMapper()
    {
        return new ConfigMapper(createConfigFactory());
    }

    public static Config createConfig()
    {
        return createConfigFactory().create();
    }

    public static Revision createRevision(String name)
    {
        return ImmutableRevision.builder()
            .name(name)
            .defaultParams(createConfig())
            .archiveType(ArchiveType.NONE)
            .userInfo(createConfig())
            .build();
    }

    public static WorkflowDefinition createWorkflow(String name)
    {
        return WorkflowDefinition.of(
                name,
                createConfig().set("+uniq", createConfig().set("sh>", "echo " + System.nanoTime())),
                ZoneId.of("UTC"));
    }

    public interface MayConflict
    {
        void run() throws ResourceConflictException;
    }

    public interface MayNotFound
    {
        void run() throws ResourceNotFoundException;
    }

    public interface Propagator
    {
        void run() throws Exception;
    }

    public static <X extends Throwable> void propagateOnly(Class<X> declaredType, Propagator r)
        throws X
    {
        try {
            r.run();
        }
        catch (Exception ex) {
            Throwables.propagateIfInstanceOf((Throwable) ex, declaredType);
            throw Throwables.propagate(ex);
        }
    }

    public static void assertNotFound(MayNotFound r)
    {
        try {
            r.run();
            fail();
        }
        catch (ResourceNotFoundException ex) {
        }
    }

    public static void assertConflict(MayConflict r)
    {
        try {
            r.run();
            fail();
        }
        catch (ResourceConflictException ex) {
        }
    }

    public static void assertNotConflict(MayConflict r)
    {
        try {
            r.run();
        }
        catch (ResourceConflictException ex) {
            fail();
        }
    }

    public static void assertEmpty(Collection<?> r)
    {
        assertTrue(r.isEmpty());
    }
}
