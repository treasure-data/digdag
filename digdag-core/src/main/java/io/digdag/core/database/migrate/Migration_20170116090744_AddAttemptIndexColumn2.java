package io.digdag.core.database.migrate;

import java.util.List;
import org.jdbi.v3.core.Handle;

public class Migration_20170116090744_AddAttemptIndexColumn2
        implements Migration
{
    @Override
    public void migrate(Handle handle, MigrationContext context)
    {
        if (context.isPostgres()) {
            handle.execute(
                    "update session_attempts set index = seq.index " +
                    "from (" +
                        "select id, rank() over (partition by session_id order by id) as index from session_attempts" +
                    ") seq " +
                    "where session_attempts.id = seq.id");
        }
        else {
            List<IdAndSessionId> list =
                handle.createQuery("select id, session_id from session_attempts order by session_id, id")
                .map((r, ctx) -> new IdAndSessionId(r.getLong("id"), r.getLong("session_id")))
                .list();
            long lastSessionId = 0L;
            long lastIndex = 0;
            for (IdAndSessionId s : list) {
                if (lastSessionId != s.sessionId) {
                    lastSessionId = s.sessionId;
                    lastIndex = 0;
                }
                lastIndex++;
                handle.createUpdate("update session_attempts set index = :index where id = :id")
                    .bind("id", s.id)
                    .bind("index", lastIndex)
                    .execute();
            }
        }

        handle.execute("alter table session_attempts" +
                " alter column index set not null");

        handle.execute("create unique index session_attempts_on_session_id_and_index on session_attempts (session_id, index desc)");
    }

    private static class IdAndSessionId
    {
        long id;
        long sessionId;

        IdAndSessionId(long id, long sessionId)
        {
            this.id = id;
            this.sessionId = sessionId;
        }
    }
}
