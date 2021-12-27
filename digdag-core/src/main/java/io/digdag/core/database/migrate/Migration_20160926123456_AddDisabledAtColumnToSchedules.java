package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20160926123456_AddDisabledAtColumnToSchedules
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.execute("alter table schedules" +
                    " add column disabled_at timestamp with time zone");
        }
        else {
            handle.execute("alter table schedules" +
                    " add column disabled_at timestamp");
        }

        if (context.isPostgres()) {
            handle.execute("drop index schedules_on_next_run_time");
            handle.execute("create index schedules_on_next_run_time on schedules (next_run_time) where disabled_at is null");
        }
    }
}
