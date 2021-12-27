package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20160623123456_AddUserInfoColumnToRevisions
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.execute("alter table revisions" +
                " add column user_info text");
    }
}
