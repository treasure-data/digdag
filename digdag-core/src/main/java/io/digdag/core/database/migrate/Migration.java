package io.digdag.core.database.migrate;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.jdbi.v3.core.Handle;

public interface Migration
{
    static Pattern MIGRATION_NAME_PATTERN = Pattern.compile("Migration_([0-9]{14})_([A-Za-z0-9]+)");

    default String getVersion()
    {
        Matcher m = MIGRATION_NAME_PATTERN.matcher(getClass().getSimpleName());
        if (!m.matches()) {
            throw new AssertionError("Invalid migration class name: " + getClass().getSimpleName());
        }
        return m.group(1);
    }

    /**
     * If true, this Migration apply without transaction.
     * This is introduced specifically for PostgreSQL's CREATE INDEX CONCURRENTLY statement.
     *
     * If this method returns true, the migration MUST RUN ONLY ONE DDL STATEMENT.
     * Otherwise, a failure in the middle of multiple statements causes inconsistency, and
     * retrying the migration may never success because the first DDL statement is already
     * applied and next migration attempt will also apply the same DDL statement.
     *
     * @return
     */
    default boolean noTransaction(MigrationContext context)
    {
        return false;
    }

    void migrate(Handle handle, MigrationContext context);
}
