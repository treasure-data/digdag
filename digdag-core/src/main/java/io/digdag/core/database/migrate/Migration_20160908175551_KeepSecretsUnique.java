package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160908175551_KeepSecretsUnique
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            // make sure records are unique
            handle.update("delete from secrets where id = any(select id from (select row_number() over (partition by site_id, project_id, scope, key order by id) as i, id from secrets) win where i > 1)");
        }
        else {
            // h2 doesn't support window function...
        }
        handle.update("create unique index secrets_unique_on_site_id_and_project_id_and_scope_and_key on secrets (site_id, project_id, scope, key)");
        handle.update("drop index secrets_on_site_id_and_project_id_and_scope_and_key");
    }
}
