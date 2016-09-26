package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160926123456_AddDisabledColumnToSchedules
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update("alter table schedules add column disabled_at bigint");
    }
}
