package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20161209001857_CreateDelayedSessionAttempts
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update(
                context.newCreateTableBuilder("delayed_session_attempts")
                .addLongIdNoAutoIncrement("id", "references session_attempts (id)")
                .addLong("dependent_session_id", "")
                .addLong("next_run_time", "not null")
                .addTimestamp("updated_at", "not null")
                .build());
        handle.update("create index delayed_session_attempts_on_next_run_time on delayed_session_attempts (next_run_time)");
    }
}
