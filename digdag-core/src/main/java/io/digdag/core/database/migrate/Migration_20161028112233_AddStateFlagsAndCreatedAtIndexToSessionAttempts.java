package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20161028112233_AddStateFlagsAndCreatedAtIndexToSessionAttempts
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        // for DatabaseSessionStoreManager.findActiveAttemptsCreatedBefore. This includes all running attempts excepting CANCEL_REQUESTED (flag=0x01)
        if (context.isPostgres()) {
            handle.execute("create index session_attempts_on_state_flags_and_created_at on session_attempts (created_at, id) where state_flags = 0");
        } else {
            handle.execute("create index session_attempts_on_state_flags_and_created_at on session_attempts (state_flags, created_at, id)");
        }
    }
}
