package io.digdag.core.database;

import java.util.List;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.*;
import io.digdag.core.queue.QueueDescStore;
import io.digdag.core.queue.QueueDescStoreManager;
import io.digdag.core.queue.StoredQueueDesc;
import io.digdag.core.queue.ImmutableStoredQueueDesc;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import io.digdag.core.config.Config;

public class DatabaseQueueDescStoreManager
        extends BasicDatabaseStoreManager
        implements QueueDescStoreManager
{
    private final ConfigMapper cfm;
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseQueueDescStoreManager(IDBI dbi, ConfigMapper cfm)
    {
        this.handle = dbi.open();
        this.cfm = cfm;
        handle.registerMapper(new StoredQueueDescMapper(cfm));
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

        public StoredQueueDesc getQueueDescByNameOrCreateDefault(String name, Config defaultConfig)
        {
            StoredQueueDesc desc = getQueueDescByName(name);
            if (desc == null) {
                // TODO transaction
                dao.insertQueueDesc(siteId, name, defaultConfig);
                desc = getQueueDescByName(name);
            }
            return desc;
        }

        public void updateQueueDescConfig(long qdId, Config newConfig)
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
        long insertQueueDesc(@Bind("siteId") int siteId, @Bind("name") String name, @Bind("config") Config config);

        @SqlUpdate("update queues" +
                " set config = :config, updated_at = now()" +
                " where site_id = :siteId " +
                " and id = :id")
        int updateQueueDescConfig(@Bind("siteId") int siteId, @Bind("id") long id, @Bind("config") Config config);
    }

    private static class StoredQueueDescMapper
            implements ResultSetMapper<StoredQueueDesc>
    {
        private final ConfigMapper cfm;

        public StoredQueueDescMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredQueueDesc map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredQueueDesc.builder()
                .id(r.getInt("id"))
                .siteId(r.getInt("site_id"))
                .createdAt(r.getTimestamp("created_at"))
                .updatedAt(r.getTimestamp("updated_at"))
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .build();
        }
    }
}
