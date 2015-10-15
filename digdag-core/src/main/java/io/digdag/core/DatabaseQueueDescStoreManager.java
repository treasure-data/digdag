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

public class DatabaseQueueDescStoreManager
        extends BasicDatabaseStoreManager
        implements QueueDescStoreManager
{
    private final ConfigSourceMapper cfm;
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseQueueDescStoreManager(IDBI dbi, ConfigSourceMapper cfm)
    {
        this.handle = dbi.open();
        this.cfm = cfm;
        handle.registerArgumentFactory(cfm.getArgumentFactory());
        this.dao = handle.attach(Dao.class);
    }

    public void close()
    {
        handle.close();
    }

    public QueueDescStore getQueueDescStore(int siteId)
    {
        return new DatabaseQueueDescStore(siteId);
    }

    private class DatabaseQueueDescStore
            implements QueueDescStore
    {
        // TODO retry
        private final int siteId;

        public DatabaseQueueDescStore(int siteId)
        {
            this.siteId = siteId;
        }

        public <T> T transaction(StoreTransaction<T> transaction)
        {
            return handle.inTransaction((handle, session) -> transaction.call());
        }

        public List<StoredQueueDesc> getAllQueueDescs()
        {
            return dao.getQueueDescs(siteId, Integer.MAX_VALUE, 0);
        }

        public List<StoredQueueDesc> getQueueDescs(int pageSize, Optional<Long> lastId)
        {
            return dao.getQueueDescs(siteId, pageSize, lastId.or(0L));
        }

        public StoredQueueDesc getQueueDescById(long qdId)
        {
            return dao.getQueueDescById(siteId, qdId);
        }

        public StoredQueueDesc getQueueDescByName(String name)
        {
            return dao.getQueueDescByName(siteId, name);
        }

        public StoredQueueDesc getQueueDescOrCreateDefault(String name, ConfigSource defaultConfig)
        {
            StoredQueueDesc desc = getQueueDescByName(name);
            // TODO create if not exists
            return desc;
        }

        public void updateQueueDescConfig(long qdId, ConfigSource newConfig)
        {
            int n = dao.updateQueueDescConfig(siteId, qdId, newConfig);
            if (n <= 0) {
                // TODO throw not-found exception
            }
        }
    }

    public interface Dao
    {
        @SqlQuery("select * from queues" +
                " where site_id = :siteId" +
                " and id > :lastId" +
                " order by id desc" +
                " limit :limit")
        List<StoredQueueDesc> getQueueDescs(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select * from queues" +
                " where site_id = :siteId" +
                " and id = :id" +
                " limit 1")
        StoredQueueDesc getQueueDescById(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select * from queues" +
                " where site_id = :siteId" +
                " and name = :name" +
                " limit 1")
        StoredQueueDesc getQueueDescByName(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlUpdate("insert into queues" +
                " (site_id, name, config, created_at, updated_at)" +
                " values (:siteId, :name, :config, now(), now())")
        @GetGeneratedKeys
        long insertQueueDesc(@Bind("siteId") int siteId, @Bind("name") String name, @Bind("config") ConfigSource config);

        @SqlUpdate("update queues" +
                " set config = :config" +
                " where site_id = :siteId " +
                " and id = :id")
        int updateQueueDescConfig(@Bind("siteId") int siteId, @Bind("id") long id, @Bind("config") ConfigSource config);
    }
}
