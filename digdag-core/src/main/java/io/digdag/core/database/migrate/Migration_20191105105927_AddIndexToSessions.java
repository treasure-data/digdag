package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20191105105927_AddIndexToSessions
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.execute("create index concurrently sessions_on_project_id_and_workflow_name_desc on sessions (project_id, workflow_name, id DESC)");
        } else {
            handle.execute("create index sessions_on_project_id_and_workflow_name_desc on sessions (project_id, workflow_name, id DESC)");
        }
    }

    @Override
    public boolean noTransaction(MigrationContext context)
    {
        return context.isPostgres();
    }
}
