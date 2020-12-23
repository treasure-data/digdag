package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20191105105927_AddIndexToSessions
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.update("create index concurrently sessions_on_project_id_and_workflow_name_desc on sessions (project_id, workflow_name, id DESC)");
        } else {
            handle.update("create index sessions_on_project_id_and_workflow_name_desc on sessions (project_id, workflow_name, id DESC)");
        }
    }

    @Override
    public boolean noTransaction(MigrationContext context)
    {
        return context.isPostgres();
    }
}
