package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20160610154832_MakeProjectsDeletable
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.execute("alter table projects" +
                    " add column deleted_at timestamp with time zone");
            handle.execute("alter table projects" +
                    " add column deleted_name text");
            handle.execute("alter table projects" +
                    " alter column name drop not null");
        }
        else {
            handle.execute("alter table projects" +
                    " add column deleted_at timestamp");
            handle.execute("alter table projects" +
                    " add column deleted_name varchar(255)");
            handle.execute("alter table projects" +
                    " alter column name drop not null");
        }
    }
}
