package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160602123456_SessionsOnProjectIdIndexToDesc
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update("create index sessions_on_project_id_desc on sessions (project_id, id desc)");
        handle.update("drop index sessions_on_project_id");
    }
}
