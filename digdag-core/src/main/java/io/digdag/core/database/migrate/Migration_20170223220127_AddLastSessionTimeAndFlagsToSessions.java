package io.digdag.core.database.migrate;

import org.skife.jdbi.v2.Handle;

public class Migration_20170223220127_AddLastSessionTimeAndFlagsToSessions
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.update("alter table sessions" +
                    " add column last_attempt_created_at timestamp with time zone");
            handle.update(
                    "update sessions" +
                    " set last_attempt_created_at = session_attempts.created_at" +
                    " from session_attempts" +
                    " where session_attempts.id = sessions.last_attempt_id");
        }
        else {
            handle.update("alter table sessions" +
                    " add column last_attempt_created_at timestamp");
            handle.update("update sessions" +
                    " set last_attempt_created_at = (" +
                        " select created_at from session_attempts" +
                        " where session_attempts.id = sessions.last_attempt_id" +
                    ")");
        }

        handle.update("create index sessions_on_project_id_and_workflow_name_and_last_attempt_created_at on sessions (project_id, workflow_name, last_attempt_created_at desc)");
    }
}
