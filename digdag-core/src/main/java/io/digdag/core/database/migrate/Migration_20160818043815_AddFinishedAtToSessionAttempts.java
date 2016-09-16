package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160818043815_AddFinishedAtToSessionAttempts
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.update("alter table session_attempts" +
                    " add column finished_at timestamp with time zone");
        }
        else {
            handle.update("alter table session_attempts" +
                    " add column finished_at timestamp");
        }
    }
}
