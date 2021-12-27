package io.digdag.core.database.migrate;

import java.util.List;
import org.jdbi.v3.core.Handle;

public class Migration_20170116082921_AddAttemptIndexColumn1
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        handle.execute("alter table session_attempts" +
                " add column index int");
    }
}
