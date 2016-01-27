package io.digdag.core.database;

import java.util.List;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.digdag.client.config.Config;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public class DatabaseQueueDescStoreManager
        extends BasicDatabaseStoreManager<DatabaseQueueDescStoreManager.Dao>
        implements QueueDescStoreManager
{
    @Inject
    public DatabaseQueueDescStoreManager(IDBI dbi, ConfigMapper cfm, DatabaseStoreConfig config)
    {
        super(config.getType(), Dao.class, () -> {
            Handle handle = dbi.open();
            handle.registerMapper(new StoredQueueDescMapper(cfm));
            handle.registerArgumentFactory(cfm.getArgumentFactory());
            return handle;
        });
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

        //public List<StoredQueueDesc> getAllQueueDescs()
        //{
        //    return dao.getQueueDescs(siteId, Integer.MAX_VALUE, 0);
        //}

        @Override
        public List<StoredQueueDesc> getQueueDescs(int pageSize, Optional<Long> lastId)
        {
            return autoCommit((handle, dao) -> {
                return dao.getQueueDescs(siteId, pageSize, lastId.or(0L));
            });
        }

        @Override
        public StoredQueueDesc getQueueDescById(long qdId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getQueueDescById(siteId, qdId),
                    "queue id=%d", qdId);
        }

        @Override
        public StoredQueueDesc getQueueDescByName(String name)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getQueueDescByName(siteId, name),
                    "queue name=%s", name);
        }

        @Override
        public StoredQueueDesc getQueueDescByNameOrCreateDefault(String name, Config defaultConfig)
        {
            return transaction((handle, dao, ts) -> {
                try {
                    try {
                        long qdId = catchConflict(() ->
                                dao.insertQueueDesc(siteId, name, defaultConfig),
                                "queue name=%s", name);
                        return requiredResource(
                                dao.getQueueDescById(siteId, qdId),
                                "queue id=%d", qdId);
                                }
                    catch (ResourceConflictException ex) {
                        return getQueueDescByName(name);
                    }
                }
                catch (ResourceNotFoundException ex) {
                    throw new IllegalStateException("Database state error", ex);
                }
            });
        }

        public void updateQueueDescConfig(long qdId, Config newConfig)
        {
            int n = autoCommit((handle, dao) -> dao.updateQueueDescConfig(siteId, qdId, newConfig));
            if (n <= 0) {
                // TODO throw not-found exception?
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
                .createdAt(getTimestampInstant(r, "created_at"))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .build();
        }
    }
}
