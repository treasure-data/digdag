package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20161028112233_AddStateFlagsAndCreatedAtIndexToSessionAttempts
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update("create index session_attempts_on_state_flags_and_created_at on session_attempts (state_flags, created_at, id desc)");
    }
}
