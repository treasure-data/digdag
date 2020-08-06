package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20200716114008_AddLastAttemptIdIndexToSessions
    implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.update("create index concurrently sessions_on_last_attempt_id on sessions (last_attempt_id)");
        } else {
            handle.update("create index sessions_on_last_attempt_id on sessions (last_attempt_id)");
        }
    }

    @Override
    public boolean noTransaction(MigrationContext context)
    {
        return context.isPostgres();
    }
}
