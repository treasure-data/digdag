package io.digdag.core.database;

import com.google.inject.Inject;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.StatementException;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.digdag.core.database.migrate.*;

public class DatabaseMigrator
{
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

    private static enum NextStatus
    {
        RUN_ALL,
        FINISHED,
        MORE;
    }

    public void migrate()
    {
        MigrationContext context = new MigrationContext(databaseType);

        while (true) {
            NextStatus status;

            try (Handle handle = dbi.open()) {
                status = handle.inTransaction((h, session) -> {
                    Set<String> appliedSet;

                    try {
                        if (context.isPostgres()) {
                            // lock tables not to run migration concurrently.
                            // h2 doesn't support table lock.
                            h.update("LOCK TABLE schema_migrations IN EXCLUSIVE MODE");
                        }

                        appliedSet = new HashSet<>(
                                handle.createQuery("select name from schema_migrations")
                                .mapTo(String.class)
                                .list());
                    }
                    catch (StatementException ex) {
                        // schema_migrations table does not exist. this is the initial run
                        // including local mode. apply everything at once.
                        return NextStatus.RUN_ALL;
                    }

                    for (Migration m : migrations) {
                        if (appliedSet.add(m.getVersion())) {
                            System.out.println("applying " + m.getVersion() + " set: " + appliedSet);
                            applyMigration(m, h, context);
                            // use isolated transaction for each migration script to avoid long lock.
                            return NextStatus.MORE;
                        }
                    }

                    return NextStatus.FINISHED;
                });
            }

            switch (status) {
            case FINISHED:
                return;

            case RUN_ALL:
                try (Handle handle = dbi.open()) {
                    handle.inTransaction((h, session) -> {
                        createSchemaMigrationsTable(h, context);
                        for (Migration m : migrations) {
                            applyMigration(m, h, context);
                        }
                        return true;
                    });
                }
                break;

            case MORE:
                // pass-through
            }
        }
    }

    private void createSchemaMigrationsTable(Handle handle, MigrationContext context)
    {
        handle.update(
                context.newCreateTableBuilder("schema_migrations")
                .addString("name", "not null")
                .addTimestamp("created_at", "not null")
                .build());
    }

    private void applyMigration(Migration m, Handle handle, MigrationContext context)
    {
        m.migrate(handle, context);
        handle.insert("insert into schema_migrations (name, created_at) values (?, now())", m.getVersion());
    }
}
