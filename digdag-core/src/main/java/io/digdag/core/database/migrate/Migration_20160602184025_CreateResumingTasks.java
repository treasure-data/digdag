package io.digdag.core.database.migrate;

import org.jdbi.v3.core.Handle;

public class Migration_20160602184025_CreateResumingTasks
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        // resuming_tasks
        handle.execute(
                context.newCreateTableBuilder("resuming_tasks")
                .addLongId("id")
                .addLong("attempt_id", "not null references session_attempts (id)")
                .addLong("source_task_id", "not null")
                .addMediumText("full_name", "not null")
                .addTimestamp("updated_at", "not null")
                .addMediumText("local_config", "")
                .addMediumText("export_config", "")
                .addMediumText("subtask_config", "")
                .addMediumText("export_params", "")
                .addMediumText("store_params", "")
                .addMediumText("report", "")
                .addMediumText("error", "")
                .build());
        if (context.isPostgres()) {
            handle.execute("create index resuming_tasks_on_attempt_id_and_full_name on resuming_tasks (attempt_id, full_name)");
        }
        else {
            // h2 doesn't support index on text
            handle.execute("create index resuming_tasks_on_attempt_id on resuming_tasks (attempt_id)");
        }

        // task_details.resuming_task_id
        handle.execute("alter table task_details" +
                " add column resuming_task_id bigint");
    }
}
