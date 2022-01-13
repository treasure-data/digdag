package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20161110112233_AddStartedAtColumnAndIndexToTasks
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.execute("alter table tasks" +
                    " add column started_at timestamp with time zone");
        }
        else {
            handle.execute("alter table tasks" +
                    " add column started_at timestamp");
        }

        if (context.isPostgres()) {
            handle.execute("create index tasks_on_state_and_started_at on tasks (state, started_at, id asc) where started_at is not null");
        } else {
            handle.execute("create index tasks_on_state_and_started_at on tasks (state, started_at, id asc)");
        }
    }
}
