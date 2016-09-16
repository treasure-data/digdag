package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160610154832_MakeProjectsDeletable
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.update("alter table projects" +
                    " add column deleted_at timestamp with time zone");
            handle.update("alter table projects" +
                    " add column deleted_name text");
            handle.update("alter table projects" +
                    " alter column name drop not null");
        }
        else {
            handle.update("alter table projects" +
                    " add column deleted_at timestamp");
            handle.update("alter table projects" +
                    " add column deleted_name varchar(255)");
            handle.update("alter table projects" +
                    " alter column name drop not null");
        }
    }
}
