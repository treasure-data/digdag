package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160928203753_AddWorkflowOrderIndex
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        // DatabaseProjectStoreManager.PgDao.getLatestActiveWorkflowDefinitions uses this index.
        handle.update("create index workflow_definitions_on_revision_id_and_id on workflow_definitions (revision_id, id);");
    }
}
