package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160926123456_AddDisabledAtColumnToSchedules
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.update("alter table schedules" +
                    " add column disabled_at timestamp with time zone");
        }
        else {
            handle.update("alter table schedules" +
                    " add column disabled_at timestamp");
        }

        if (context.isPostgres()) {
            handle.update("drop index schedules_on_next_run_time");
            handle.update("create index schedules_on_next_run_time on schedules (next_run_time) where disabled_at is null");
        }
    }
}
