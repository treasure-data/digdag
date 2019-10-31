package io.digdag.core.database.migrate;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.skife.jdbi.v2.Handle;

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
     * This is introduced because 'create index concurrently' cannot run in transaction.
     * @return
     */
    default boolean noTransaction(MigrationContext context)
    {
        return false;
    }

    void migrate(Handle handle, MigrationContext context);
}
