package io.digdag.core.database;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.StatementException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.digdag.core.database.migrate.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseMigrator
{
    private static final Logger logger = LoggerFactory.getLogger(DatabaseMigrator.class);

    private final List<Migration> migrations = Stream.of(new Migration[] {
        new Migration_20151204221156_CreateTables(),
        new Migration_20160602123456_SessionsOnProjectIdIndexToDesc(),
        new Migration_20160602184025_CreateResumingTasks(),
        new Migration_20160610154832_MakeProjectsDeletable(),
        new Migration_20160623123456_AddUserInfoColumnToRevisions(),
        new Migration_20160719172538_QueueRearchitecture(),
        new Migration_20160817123456_AddSecretsTable(),
        new Migration_20160818043815_AddFinishedAtToSessionAttempts(),
        new Migration_20160818220026_QueueUniqueName(),
        new Migration_20160908175551_KeepSecretsUnique(),
        new Migration_20160926123456_AddDisabledAtColumnToSchedules(),
        new Migration_20160928203753_AddWorkflowOrderIndex(),
        new Migration_20161005225356_AddResetParamsToTaskState(),
        new Migration_20161028112233_AddStateFlagsAndCreatedAtIndexToSessionAttempts(),
        new Migration_20161110112233_AddStartedAtColumnAndIndexToTasks(),
        new Migration_20161209001857_CreateDelayedSessionAttempts(),
        new Migration_20170116082921_AddAttemptIndexColumn1(),
        new Migration_20170116090744_AddAttemptIndexColumn2(),
        new Migration_20170223220127_AddLastSessionTimeAndFlagsToSessions(),
        new Migration_20190318175338_AddIndexToSessionAttempts(),
    })
    .sorted(Comparator.comparing(m -> m.getVersion()))
    .collect(Collectors.toList());

    private final DBI dbi;
    private final String databaseType;

    @Inject
    public DatabaseMigrator(DBI dbi, DatabaseConfig config)
    {
        this(dbi, config.getType());
    }

    public DatabaseMigrator(DBI dbi, String databaseType)
    {
        this.dbi = dbi;
        this.databaseType = databaseType;
    }

    public static String getDriverClassName(String type)
    {
        switch (type) {
        case "h2":
            return "org.h2.Driver";
        case "postgresql":
            return "org.postgresql.Driver";
        default:
            throw new RuntimeException("Unsupported database type: "+type);
        }
    }

    public String getSchemaVersion()
    {
        try (Handle handle = dbi.open()) {
            return handle.createQuery("select name from schema_migrations order by name desc limit 1")
                .mapTo(String.class)
                .first();
        }
    }

    public void migrateWithRetry()
    {
        final int maxRetry = 3;
        for (int i = 0; i < maxRetry; i++) {
            try {
                logger.info("Database migration started");
                migrate();
                logger.info("Database migration successfully finished.");
                return;
            }
            catch (RuntimeException re) {
                logger.warn(re.toString());
                if (i == maxRetry - 1) {
                    logger.error("Critical error!!. Database migration failed.");
                }
                else {
                    logger.warn("Database migration failed. Retry");
                    try {
                        Thread.sleep(30 * 1000);
                    }
                    catch (InterruptedException ie) {

                    }
                }
            }
        }
        logger.error("Database migration aborted.");
    }

    // Call from cli
    public int migrate()
    {
        int numApplied = 0;
        MigrationContext context = new MigrationContext(databaseType);
        try (Handle handle = dbi.open()) {
            boolean isInitial = !existsSchemaMigrationsTable(handle);
            if (isInitial) {
                createSchemaMigrationsTable(handle, context);
            }

            for (Migration m : migrations) {
                Set<String> appliedSet = getAppliedMigrationNames(handle);
                if (appliedSet.add(m.getVersion())) {
                    logger.info("Applying database migration:" + m.getVersion());
                    numApplied++;
                    if (m.noTransaction()) {
                        // In no transaction we can't lock schema_migrations table
                        applyMigration(m, handle, context);
                    }
                    else {
                        handle.inTransaction((h, session) -> {
                            if (context.isPostgres()) {
                                // lock tables not to run migration concurrently.
                                // h2 doesn't support table lock.
                                h.update("LOCK TABLE schema_migrations IN EXCLUSIVE MODE");
                            }
                            applyMigration(m, handle, context);
                            return true;
                        });
                    }
                }
            }
        }
        return numApplied;
    }

    // Called from cli migrate

    /**
     * Called from cli migrate
     * @return no applicated migrations.
     */
    public List<Migration> getApplicableMigration()
    {
        List<Migration> applicableMigrations = new ArrayList<>();
        MigrationContext context = new MigrationContext(databaseType);
        try (Handle handle = dbi.open()) {
            boolean isInitial = !existsSchemaMigrationsTable(handle);
            if (isInitial) {
                return applicableMigrations;
            }

            Set<String> appliedSet = getAppliedMigrationNames(handle);
            for (Migration m : migrations) {
                if (!appliedSet.contains(m.getVersion())) {
                    applicableMigrations.add(m);
                }
            }
        }
        return applicableMigrations;
    }

    Set<String> getAppliedMigrationNames(Handle handle)
    {
        return new HashSet<>(
                handle.createQuery("select name from schema_migrations")
                        .mapTo(String.class)
                        .list());
    }

    @VisibleForTesting
    public void createSchemaMigrationsTable(Handle handle, MigrationContext context)
    {
        handle.update(
                context.newCreateTableBuilder("schema_migrations")
                .addString("name", "not null")
                .addTimestamp("created_at", "not null")
                .build());
    }

    // Called from cli migrate
    public boolean existsSchemaMigrationsTable()
    {
        try (Handle handle = dbi.open()) {
            return existsSchemaMigrationsTable(handle);
        }
    }

    private boolean existsSchemaMigrationsTable(Handle handle)
    {
        try {
            handle.createQuery("select name from schema_migrations limit 1")
                    .mapTo(String.class)
                    .list();
            return true;
        }
        catch( RuntimeException re) {
            return false;
        }
    }

    @VisibleForTesting
    public void applyMigration(Migration m, Handle handle, MigrationContext context)
    {
        m.migrate(handle, context);
        handle.insert("insert into schema_migrations (name, created_at) values (?, now())", m.getVersion());
    }

    @VisibleForTesting
    public String getDatabaseType()
    {
        return databaseType;
    }
}
