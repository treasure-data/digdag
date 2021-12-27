package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20161005225356_AddResetParamsToTaskState
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.execute("alter table task_state_details" +
                " add column reset_store_params text");
        handle.execute("alter table resuming_tasks" +
                " add column reset_store_params text");
    }
}
