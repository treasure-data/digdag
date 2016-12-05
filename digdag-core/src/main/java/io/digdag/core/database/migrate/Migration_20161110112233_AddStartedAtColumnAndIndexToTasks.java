package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20161110112233_AddStartedAtColumnAndIndexToTasks
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.update("alter table tasks" +
                    " add column started_at timestamp with time zone");
        }
        else {
            handle.update("alter table tasks" +
                    " add column started_at timestamp");
        }

        if (context.isPostgres()) {
            handle.update("create index tasks_on_state_and_started_at on tasks (state, started_at, id asc) where started_at is not null");
        } else {
            handle.update("create index tasks_on_state_and_started_at on tasks (state, started_at, id asc)");
        }
    }
}
