package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20221021035534_CreateProjectMetadata
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update(
                context.newCreateTableBuilder("project_metadata")
                .addIntId("id")
                .addInt("site_id", "not null")
                .addInt("project_id", "not null references projects (id)")
                .addString("key", "not null")
                .addString("value", "not null")
                .addTimestamp("created_at", "not null")
                .build());

        handle.update("create unique index project_metadata_on_key_and_value_and_project_id_and_site_id on project_metadata(key, value, project_id, site_id)");
        handle.update("create index project_metadata_on_site_id_and_key_and_value on project_metadata(site_id, key, value)");
        handle.update("create index project_metadata_on_project_id_and_site_id_and_key on project_metadata(project_id, site_id, key)");
    }
}
