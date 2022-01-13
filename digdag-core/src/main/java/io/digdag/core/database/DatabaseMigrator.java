package io.digdag.core.database;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.Handle;

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
        new Migration_20191105105927_AddIndexToSessions(),
        new Migration_20200716114008_AddLastAttemptIdIndexToSessions(),
        new Migration_20200803184355_ReplacePartialIndexOnSessionAttempts(),
    })
    .sorted(Comparator.comparing(m -> m.getVersion()))
    .collect(Collectors.toList());

    private final Jdbi dbi;
    private final String databaseType;

    @Inject
    public DatabaseMigrator(Jdbi dbi, DatabaseConfig config)
    {
        this(dbi, config.getType());
    }

    DatabaseMigrator(Jdbi dbi, String databaseType)
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

    public int migrate()
    {
        int numApplied = 0;
        MigrationContext context = new MigrationContext(databaseType);
        Set<String> appliedSet;
        try (Handle handle = dbi.open()) {
            boolean isInitial = !existsSchemaMigrationsTable(handle);
            if (isInitial) {
                createSchemaMigrationsTable(handle, context);
            }
            appliedSet = getAppliedMigrationNames(handle);
        }
        for (Migration m : migrations) {
            if (appliedSet.add(m.getVersion())) {
                if (applyMigrationIfNotDone(context, m)) {
                    numApplied++;
                }
            }
        }
        if (numApplied > 0) {
            if (context.isPostgres()) {
                logger.info("{} migrations applied.", numApplied);
            }
            else {
                logger.debug("{} migrations applied.", numApplied);
            }
        }
        return numApplied;
    }

    // add "synchronized" so that multiple threads don't run the same migration on the same database
    private synchronized boolean applyMigrationIfNotDone(MigrationContext context, Migration m)
    {
        // recreate Handle for each time to be able to discard pg_advisory_lock.
        try (Handle handle = dbi.open()) {
            if (m.noTransaction(context)) {
                // Advisory lock if available -> migrate
                if (context.isPostgres()) {
                    handle.select("SELECT pg_advisory_lock(23299, 0)");
                    // re-check migration status after lock
                    if (!checkIfMigrationApplied(handle, m.getVersion())) {
                        logger.info("Applying database migration:" + m.getVersion());
                        applyMigration(m, handle, context);
                        return true;
                    }
                    return false;
                }
                else {
                    // h2 doesn't support table lock and it's unnecessary because of synchronized
                    logger.debug("Applying database migration:" + m.getVersion());
                    applyMigration(m, handle, context);
                    return true;
                }

            }
            else {
                // Start transaction -> Lock table -> Re-check migration status -> migrate
                return handle.inTransaction((h) -> {
                    if (context.isPostgres()) {
                        // lock tables not to run migration concurrently.
                        h.execute("LOCK TABLE schema_migrations IN EXCLUSIVE MODE");
                        // re-check migration status after lock
                        if (!checkIfMigrationApplied(h, m.getVersion())) {
                            logger.info("Applying database migration:" + m.getVersion());
                            applyMigration(m, handle, context);
                            return true;
                        }
                        return false;
                    }
                    else {
                        // h2 doesn't support table lock and it's unnecessary because of synchronized
                        logger.debug("Applying database migration:" + m.getVersion());
                        applyMigration(m, handle, context);
                        return true;
                    }
                });
            }
        }
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

    private Set<String> getAppliedMigrationNames(Handle handle)
    {
        return new HashSet<>(
                handle.createQuery("select name from schema_migrations")
                .mapTo(String.class)
                .list());
    }

    private boolean checkIfMigrationApplied(Handle handle, String name)
    {
        return handle.createQuery("select name from schema_migrations where name = :name limit 1")
            .bind("name", name)
            .mapTo(String.class)
            .list()
            .size() > 0;
    }

    @VisibleForTesting
    public void createSchemaMigrationsTable(Handle handle, MigrationContext context)
    {
        handle.execute(
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
        handle.execute("insert into schema_migrations (name, created_at) values (?, now())", m.getVersion());
    }

    @VisibleForTesting
    public String getDatabaseType()
    {
        return databaseType;
    }
}
