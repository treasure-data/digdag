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

        public StoredSession putSession(Session session, SessionRelation relation)
        {
            // TODO idempotent operation
            long sesId;
            if (relation.getWorkflowId().isPresent()) {
                // namespace is workflow id
                sesId = dao.insertSession(siteId, NAMESPACE_WORKFLOW_ID, relation.getWorkflowId().get(), session.getName(), session.getSessionParams());
                dao.insertSessionRelation(sesId, relation.getRepositoryId().get(), relation.getWorkflowId().get());
            }
            else if (relation.getRepositoryId().isPresent()) {
                // namespace is repository
                sesId = dao.insertSession(siteId, NAMESPACE_REPOSITORY_ID, relation.getRepositoryId().get(), session.getName(), session.getSessionParams());
                dao.insertSessionRelation(sesId, relation.getRepositoryId().get(), null);
            }
            else {
                // namespace is site
                sesId = dao.insertSession(siteId, NAMESPACE_SITE_ID, siteId, session.getName(), session.getSessionParams());
                dao.insertSessionRelation(sesId, null, null);
            }
            return getSessionById(sesId);
        }
    }

    public static short NAMESPACE_WORKFLOW_ID = (short) 3;
    public static short NAMESPACE_REPOSITORY_ID = (short) 1;
    public static short NAMESPACE_SITE_ID = (short) 0;

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

        @SqlQuery("select * from sessions where site_id = :siteId and name = :name limit 1")
        StoredSession getSessionByName(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlUpdate("insert into sessions (site_id, namespace_type, namespace_id, name, session_params, created_at)" +
                "values (:siteId, :namespaceType, :namespaceId, :name, :sessionParams, now())")
        @GetGeneratedKeys
        long insertSession(@Bind("siteId") int siteId,  @Bind("namespaceType") short namespaceType,
                @Bind("namespaceId") int namespaceId, @Bind("name") String name, @Bind("sessionParams") ConfigSource sessionParams);

        @SqlUpdate("insert into session_relations (id, repository_id, workflow_id)" +
                "values (:id, :repositoryId, :workflowId)")
        void insertSessionRelation(@Bind("id") long id,  @Bind("repositoryId") Integer repositoryId,
                @Bind("workflowId") Integer workflowId);
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
                .name(r.getString("name"))
                .sessionParams(cfm.fromResultSet(r, "session_params"))
                .build();
        }
    }
}
