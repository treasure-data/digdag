package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20220223095752_AddStartDateAndEndDateColumnsToSchedules
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update("alter table schedules" +
                " add column start_date bigint");
        handle.update("alter table schedules" +
                " add column end_date bigint");

        if (context.isPostgres()) {
            handle.update("drop index schedules_on_next_run_time");
            handle.update("create index schedules_on_next_run_time on schedules (next_run_time, start_date, end_date) where disabled_at is null");
        }
    }
}
