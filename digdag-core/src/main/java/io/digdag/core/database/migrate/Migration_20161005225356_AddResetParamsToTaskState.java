package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20161005225356_AddResetParamsToTaskState
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update("alter table task_state_details" +
                " add column reset_store_params text");
        handle.update("alter table resuming_tasks" +
                " add column reset_store_params text");
    }
}
