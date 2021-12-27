package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20200803184355_ReplacePartialIndexOnSessionAttempts
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            // Don't use "create index concurrently" here since the following "drop index" should be issued after the completion of the index creation
            handle.execute("create index session_attempts_on_state_flags_and_created_at_partial on session_attempts (id, created_at) where state_flags = 0");
            handle.execute("drop index session_attempts_on_state_flags_and_created_at");
        }
        else {
            // H2 does not support partial index
        }
    }
}
