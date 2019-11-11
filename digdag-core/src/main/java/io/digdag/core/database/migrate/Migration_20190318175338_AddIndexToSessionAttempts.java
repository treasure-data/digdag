package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20190318175338_AddIndexToSessionAttempts
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        // DatabaseSessionStoreManager.getActiveAttemptCount uses this index.
        if (context.isPostgres()) {
            handle.update("create index concurrently session_attempts_on_site_id_and_state_flags_partial_2 on session_attempts"
                    + " using btree(site_id) where state_flags & 2 = 0");
        }
        else {
            // H2 does not support partial index
        }
    }

    @Override
    public boolean noTransaction(MigrationContext context)
    {
        return context.isPostgres();
    }
}
