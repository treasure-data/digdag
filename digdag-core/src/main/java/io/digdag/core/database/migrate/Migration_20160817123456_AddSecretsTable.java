package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160817123456_AddSecretsTable
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update(
                context.newCreateTableBuilder("secrets")
                        .addLongId("id")
                        .addLong("site_id", "not null")
                        .addLong("project_id", "not null references projects (id)")
                        .addString("scope", "not null")
                        .addString("engine", "not null")
                        .addString("key", "not null")
                        .addLongText("value", "not null")
                        .addTimestamp("updated_at", "not null")
                        .build());

        handle.update("create index secrets_on_site_id_and_project_id_and_scope_and_key on secrets (site_id, project_id, scope, key)");
    }
}
