package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160623123456_AddUserInfoColumnToRevisions
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update("alter table revisions" +
                " add column user_info text");
    }
}
