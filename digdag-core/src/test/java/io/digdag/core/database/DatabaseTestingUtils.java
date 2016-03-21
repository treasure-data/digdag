package io.digdag.core.database;

import java.util.Arrays;
import java.util.Collection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.repository.*;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

public class DatabaseTestingUtils
{
    private DatabaseTestingUtils() { }

    public static DatabaseFactory setupDatabase()
    {
        DatabaseConfig config = DatabaseConfig.builder()
            .type("h2")
            .path(Optional.absent())
            .remoteDatabaseConfig(Optional.absent())
            .options(ImmutableMap.of())
            .expireLockInterval(10)
            .autoMigrate(true)
            .connectionTimeout(30)
            .idleTimeout(600)
            .validationTimeout(5)
            .maximumPoolSize(10)
            .build();
        PooledDataSourceProvider dsp = new PooledDataSourceProvider(config);

        DBI dbi = new DBI(dsp.get());
        new DatabaseMigrator(dbi, config).migrate();

        cleanDatabase(dbi);

        return new DatabaseFactory(dbi, dsp, config);
    }

    public static final String[] ALL_TABLES = new String[] {
        "repositories",
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
        "resource_types",
        "queued_tasks",
        "queued_shared_task_locks",
        "queued_task_locks",
    };

    public static void cleanDatabase(IDBI dbi)
    {
        try (Handle handle = dbi.open()) {
            // h2 database can't truncate tables with references if REFERENTIAL_INTEGRITY is true (default)
            handle.createStatement("SET REFERENTIAL_INTEGRITY FALSE").execute();
            for (String name : Lists.reverse(Arrays.asList(ALL_TABLES))) {
                handle.createStatement("TRUNCATE TABLE " + name).execute();
            }
        }
    }

    public static ObjectMapper createObjectMapper()
    {
        return new ObjectMapper();
    }

    public static ConfigFactory createConfigFactory()
    {
        return new ConfigFactory(createObjectMapper());
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
            .archiveType("none")
            .build();
    }

    public static WorkflowDefinition createWorkflow(String name)
    {
        return WorkflowDefinition.of(
                name,
                createConfig().set("uniq", System.nanoTime()));
    }

    public interface MayConflict
    {
        void run() throws ResourceConflictException;
    }

    public interface MayNotFound
    {
        void run() throws ResourceNotFoundException;
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
