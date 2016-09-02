package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20160818220026_QueueUniqueName
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.update("drop index queued_tasks_on_queue_id_task_id");
        handle.update("drop index queued_tasks_on_site_id_task_id");

        // alter "task_id bigint" to "unique_name text"
        if (context.isPostgres()) {
            handle.update("alter table queued_tasks" +
                    " alter column task_id type text");
            handle.update("alter table queued_tasks" +
                        " rename column task_id to unique_name");
        }
        else {
            handle.update("alter table queued_tasks" +
                    " alter column task_id varchar(255)");
            handle.update("alter table queued_tasks" +
                        " alter column task_id rename to unique_name");
        }

        handle.update("alter table queued_tasks" +
                    " alter unique_name set not null");

        handle.update("create unique index queued_tasks_on_site_id_unique_name on queued_tasks (site_id, unique_name)");
        if (context.isPostgres()) {
            handle.update("create unique index queued_tasks_on_queue_id_unique_name on queued_tasks (queue_id, unique_name) where queue_id is not null");
        }
        else {
            handle.update("create unique index queued_tasks_on_queue_id_unique_name on queued_tasks (queue_id, unique_name)");
        }

        handle.update("alter table tasks" +
                " add column retry_count int default 0");
    }
}
