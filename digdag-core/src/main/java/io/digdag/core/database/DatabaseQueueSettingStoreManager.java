package io.digdag.core.database;

import java.util.List;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.google.common.base.*;
import com.google.inject.Inject;
import io.digdag.core.queue.QueueSettingStore;
import io.digdag.core.queue.QueueSettingStoreManager;
import io.digdag.core.queue.StoredQueueSetting;
import io.digdag.core.queue.ImmutableStoredQueueSetting;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.mapper.RowMapper;
import io.digdag.core.repository.ResourceNotFoundException;

public class DatabaseQueueSettingStoreManager
        extends BasicDatabaseStoreManager<DatabaseQueueSettingStoreManager.Dao>
        implements QueueSettingStoreManager
{
    @Inject
    public DatabaseQueueSettingStoreManager(TransactionManager transactionManager, DatabaseConfig config, ConfigMapper cfm)
    {
        super(config.getType(), Dao.class, transactionManager, cfm);
    }

    @Override
    public QueueSettingStore getQueueSettingStore(int siteId)
    {
        return new DatabaseQueueSettingStore(siteId);
    }

    @Override
    public int getQueueIdByName(int siteId, String name)
        throws ResourceNotFoundException
    {
        return requiredResource(
                (handle, dao) -> dao.getQueueIdByName(siteId, name),
                "queue name=%d", name);
    }

    private class DatabaseQueueSettingStore
            implements QueueSettingStore
    {
        // TODO retry
        private final int siteId;

        public DatabaseQueueSettingStore(int siteId)
        {
            this.siteId = siteId;
        }

        @Override
        public List<StoredQueueSetting> getQueueSettings(int pageSize, Optional<Long> lastId)
        {
            return autoCommit((handle, dao) -> {
                return dao.getQueueSettings(siteId, pageSize, lastId.or(0L));
            });
        }

        @Override
        public StoredQueueSetting getQueueSettingById(long qId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getQueueSettingById(siteId, qId),
                    "queue id=%d", qId);
        }

        @Override
        public StoredQueueSetting getQueueSettingByName(String name)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getQueueSettingByName(siteId, name),
                    "queue name=%s", name);
        }

        // TODO add interface to update queue_settings with queues.max_concurrency and resource_types.max_concurrency
    }

    public interface Dao
    {
        @SqlQuery("select * from queue_settings" +
                " where site_id = :siteId" +
                " and id > :lastId" +
                " order by id desc" +
                " limit :limit")
        List<StoredQueueSetting> getQueueSettings(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select * from queue_settings" +
                " where site_id = :siteId" +
                " and id = :id" +
                " limit 1")
        StoredQueueSetting getQueueSettingById(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select * from queue_settings" +
                " where site_id = :siteId" +
                " and name = :name" +
                " limit 1")
        StoredQueueSetting getQueueSettingByName(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlQuery("select id from queue_settings" +
                " where site_id = :siteId" +
                " and name = :name" +
                " limit 1")
        Integer getQueueIdByName(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlUpdate("insert into queue_settings" +
                " (site_id, name, config, created_at, updated_at)" +
                " values (:siteId, :name, NULL, now(), now())")
        @GetGeneratedKeys
        int insertDefaultQueueSetting(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlUpdate("insert into queues" +
                " (id, max_concurrency)" +
                " values (:id, :maxConcurrency)")
        int insertQueue(@Bind("id") int id, @Bind("maxConcurrency") int maxConcurrency);
    }

    static class StoredQueueSettingMapper
            implements RowMapper<StoredQueueSetting>
    {
        private final ConfigMapper cfm;

        public StoredQueueSettingMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredQueueSetting map(ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredQueueSetting.builder()
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
