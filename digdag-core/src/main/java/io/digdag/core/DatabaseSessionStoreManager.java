package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public class DatabaseSessionStoreManager
        extends BasicDatabaseStoreManager
        implements SessionStoreManager
{
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseSessionStoreManager(IDBI dbi, ConfigSourceMapper cfm)
    {
        this.handle = dbi.open();
        handle.registerMapper(new StoredSessionMapper(cfm));
        handle.registerArgumentFactory(cfm.getArgumentFactory());
        this.dao = handle.attach(Dao.class);
    }

    public void close()
    {
        handle.close();
    }

    public SessionStore getSessionStore(int siteId)
    {
        return new DatabaseSessionStore(siteId);
    }

    private class DatabaseSessionStore
            implements SessionStore
    {
        // TODO retry
        private final int siteId;

        public DatabaseSessionStore(int siteId)
        {
            this.siteId = siteId;
        }

        public <T> T transaction(StoreTransaction<T> transaction)
        {
            return handle.inTransaction((handle, session) -> transaction.call());
        }

        public List<StoredSession> getAllSessions()
        {
            return dao.getSessions(siteId, Integer.MAX_VALUE, 0L);
        }

        public List<StoredSession> getSessions(int pageSize, Optional<Long> lastId)
        {
            return dao.getSessions(siteId, pageSize, lastId.or(0L));
        }

        public StoredSession getSessionById(long sesId)
        {
            return dao.getSessionById(siteId, sesId);
        }

        public StoredSession getSessionByName(String name)
        {
            return dao.getSessionByName(siteId, name);
        }

        public StoredSession putSession(Session session)
        {
            // TODO idempotent operation
            long sesId = dao.insertSession(siteId, session.getWorkflowId().orNull(), session.getUniqueName(),
                    session.getSessionParams());
            return getSessionById(sesId);
        }
    }

    public interface Dao
    {
        @SqlQuery("select * from sessions" +
                " where site_id = :siteId" +
                " and id > :lastId" +
                " order by id desc" +
                " limit :limit")
        List<StoredSession> getSessions(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select * from sessions where site_id = :siteId and id = :id limit 1")
        StoredSession getSessionById(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select * from sessions where site_id = :siteId and unique_name = :name limit 1")
        StoredSession getSessionByName(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlUpdate("insert into sessions (site_id, workflow_id, unique_name, session_params, created_at)" +
                "values (:siteId, :workflowId, :uniqueName, :sessionParams, now())")
        @GetGeneratedKeys
        long insertSession(@Bind("siteId") int siteId, @Bind("workflowId") Integer workflowId, @Bind("uniqueName") String uniqueName,
                @Bind("sessionParams") ConfigSource sessionParams);
    }

    private static class StoredSessionMapper
            implements ResultSetMapper<StoredSession>
    {
        private final ConfigSourceMapper cfm;

        public StoredSessionMapper(ConfigSourceMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredSession map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredSession.builder()
                .id(r.getLong("id"))
                .siteId(r.getInt("site_id"))
                .createdAt(r.getDate("created_at"))
                .uniqueName(r.getString("unique_name"))
                .sessionParams(cfm.fromResultSet(r, "session_params"))
                .workflowId(getOptionalInt(r, "workflow_id"))
                .build();
        }
    }
}
