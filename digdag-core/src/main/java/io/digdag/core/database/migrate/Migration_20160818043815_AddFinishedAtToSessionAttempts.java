package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20160818043815_AddFinishedAtToSessionAttempts
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.execute("alter table session_attempts" +
                    " add column finished_at timestamp with time zone");
        }
        else {
            handle.execute("alter table session_attempts" +
                    " add column finished_at timestamp");
        }
    }
}
