package io.digdag.core.database;

import java.util.List;
import java.util.stream.Collectors;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.queue.QueueSettingStore;
import io.digdag.core.queue.QueueSettingStoreManager;
import io.digdag.core.queue.StoredQueueSetting;
import io.digdag.core.queue.ImmutableStoredQueueSetting;
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
import static io.digdag.core.queue.QueueSettingStore.DEFAULT_QUEUE_NAME;

public class DatabaseTaskQueueStore
        extends BasicDatabaseStoreManager<DatabaseTaskQueueStore.Dao>
{
    private final QueueSettingStoreManager qm;

    @Inject
    public DatabaseTaskQueueStore(IDBI dbi, DatabaseConfig config, QueueSettingStoreManager qm)
    {
        super(config.getType(), Dao.class, () -> {
            Handle handle = dbi.open();
            return handle;
        });
        this.qm = qm;
    }

    // TODO support Optional<String> resourceType
    public long enqueue(int siteId, String queueName, int priority, long taskId, byte[] data)
        throws ResourceConflictException
    {
        boolean useSharedTaskQueue = queueName.equals(DEFAULT_QUEUE_NAME);
        Integer resourceTypeId = null;

        return transaction((handle, dao, ts) -> {
            int queueId = qm.getQueueIdByNameOrInsertDefault(siteId, queueName);
            long queuedTaskId = catchConflict(() ->
                dao.insertQueuedTask(queueId, priority, resourceTypeId, taskId, data),
                "queued task id=%d in queue id=%d", taskId, queueId);
            if (useSharedTaskQueue) {
                dao.insertQueuedSharedTaskLock(queuedTaskId, queueId, priority, resourceTypeId);
            }
            else {
                dao.insertQueuedTaskLock(queuedTaskId, queueId, priority, resourceTypeId);
            }
            return queuedTaskId;
        }, ResourceConflictException.class);
    }

    public void delete(int siteId, String queueName, long lockedTaskId, String agentId)
        throws ResourceNotFoundException, ResourceConflictException
    {
        boolean useSharedTaskQueue = queueName.equals(DEFAULT_QUEUE_NAME);

        this.<Boolean, ResourceNotFoundException, ResourceConflictException>transaction((handle, dao, ts) -> {
            int queueId = qm.getQueueIdByName(siteId, queueName);
            int deleted;
            if (useSharedTaskQueue) {
                deleted = dao.deleteSharedTaskLock(lockedTaskId, agentId);
            }
            else {
                deleted = dao.deleteTaskLock(lockedTaskId, agentId);
            }
            if (deleted == 0) {
                throw new ResourceConflictException("Deleting task does not exist or preempted by another agent: task id=" + lockedTaskId + " agent id=" + agentId);
            }
            dao.deleteTask(lockedTaskId);
            return true;
        }, ResourceNotFoundException.class, ResourceConflictException.class);
    }

    public List<Long> lockSharedTasks(int limit, String agentId, int lockSeconds)
    {
        // optimized implementation of
        //   select distinct queue_id as id from locks wherE hold_expire_time is null
        return transaction((handle, dao, ts) -> {
            List<Integer> queueIds = handle.createQuery(
                "with recursive t (queue_id) as (" +
                    "(" +
                        "select queue_id from queued_shared_task_locks " +
                        "where hold_expire_time is null " +
                        "order by queue_id limit 1 " +
                    ") " +
                    "union all " +
                    "select (" +
                        "select queue_id from queued_shared_task_locks " +
                        "where hold_expire_time is null and queue_id > t.queue_id " +
                        "order by queue_id limit 1" +
                    ") from t where t.queue_id is not null" +
                ") " +
                "select queue_id as id from t " +
                "where queue_id is not null"
                )
                .mapTo(int.class)
                .list();
            ImmutableList.Builder<Long> result = ImmutableList.builder();
            int remaining = limit;
            for (int queueId : queueIds) {
                List<Long> locked = tryLockTasks(handle, "queued_shared_task_locks", queueId, remaining);
                if (!locked.isEmpty()) {
                    remaining -= locked.size();
                    result.addAll(locked);
                    if (remaining <= 0) {
                        break;
                    }
                }
            }
            List<Long> locked = result.build();
            setHold(handle, "queued_shared_task_locks", locked, agentId, lockSeconds);
            return locked;
        });
    }

    public List<Long> lockTasks(int siteId, String queueName, int limit, String agentId, int lockSeconds)
        throws ResourceNotFoundException
    {
        int queueId = qm.getQueueIdByName(siteId, queueName);
        return transaction((handle, dao, ts) -> {
            List<Long> locked = tryLockTasks(handle, "queued_task_locks", queueId, limit);
            setHold(handle, "queued_task_locks", locked, agentId, lockSeconds);
            return locked;
        });
    }

    private void setHold(Handle handle, String tableName, List<Long> ids, String agentId, int lockSeconds)
    {
        // TODO use this syntax for PostgreSQL
        // (statement_timestamp() + (interval '1' second) * hold_timeout)
        handle.createStatement(
                "update " + tableName + " " +
                " set hold_expire_time = :expireTime" +
                " where id in (" +
                    ids.stream()
                    .map(it -> it.toString()).collect(Collectors.joining(", ")) +
                ")"
            )
            .bind("expireTime", Instant.now().getEpochSecond() + lockSeconds)
            .execute();
    }

    private List<Long> tryLockTasks(Handle handle, String tableName, int qId, int limit)
    {
        // TODO lock this queueId to prevent over committing

        return handle.createQuery(
            "select ks.id " +
            "from " + tableName + " ks " +
            "left join (" +
                "select rt.id " +
                "from resource_types rt " +
                "left join (" +
                    "select resource_type_id, count(*) as count " +
                    "from " + tableName + " " +
                    "where queue_id = " + qId + " " +
                    "and hold_expire_time is not null " +
                    "and resource_type_id is not null " +
                    "group by resource_type_id" +
                ") rr on rt.id = rr.resource_type_id " +
                "where rt.max_concurrency <= coalesce(count, 0)" +
            ") rc on ks.resource_type_id = rc.id " +
            "where queue_id = (" +
                "select " + qId + " from queues " +
                "where id = " + qId + " " +
                "and max_concurrency > (" +
                    "select count(*) as count " +
                    "from " + tableName + " " +
                    "where queue_id = " + qId + " " +
                    "and hold_expire_time is not null " +
                ") " +
            ") " +
            "and hold_expire_time is null " +
            "and rc.id is null " +
            "order by priority desc, id asc " +
            "limit " + limit
            )
            .mapTo(long.class)
            .list();

    }

    public byte[] getTaskData(long lockedTaskId)
        throws ResourceNotFoundException
    {
        return autoCommit((handle, dao) -> dao.getTaskData(lockedTaskId));
    }

    public interface Dao
    {
        @SqlUpdate("insert into queued_tasks" +
                " (queue_id, priority, resource_type_id, task_id, data, created_at)" +
                " values (:queueId, :priority, :resourceTypeId, :taskId, :data, now())")
        @GetGeneratedKeys
        long insertQueuedTask(@Bind("queueId") int queueId, @Bind("priority") int priority,
                @Bind("resourceTypeId") Integer resourceTypeId, @Bind("taskId") long taskId,
                @Bind("data") byte[] data);

        @SqlUpdate("insert into queued_shared_task_locks" +
                " (id, queue_id, priority, resource_type_id, retry_count, hold_expire_time)" +
                " values (:id, :queueId, :priority, :resourceTypeId, 0, NULL)")
        int insertQueuedSharedTaskLock(@Bind("id") long id,
                @Bind("queueId") int queueId, @Bind("priority") int priority,
                @Bind("resourceTypeId") Integer resourceTypeId);

        @SqlUpdate("insert into queued_task_locks" +
                " (id, queue_id, priority, resource_type_id, retry_count, hold_expire_time)" +
                " values (:id, :queueId, :priority, :resourceTypeId, 0, NULL)")
        int insertQueuedTaskLock(@Bind("id") long id,
                @Bind("queueId") int queueId, @Bind("priority") int priority,
                @Bind("resourceTypeId") Integer resourceTypeId);

        @SqlQuery("select data from queued_tasks where id = :taskId")
        byte[] getTaskData(@Bind("taskId") long taskId);

        @SqlUpdate("delete from queued_shared_task_locks" +
                " where id = :taskId" +
                " and hold_agent_id = :agentId")
        int deleteSharedTaskLock(@Bind("taskId") long taskId, @Bind("agentId") String agentId);

        @SqlUpdate("delete from queued_task_locks" +
                " where id = :taskId" +
                " and hold_agent_id = :agentId")
        int deleteTaskLock(@Bind("taskId") long taskId, @Bind("agentId") String agentId);

        @SqlUpdate("delete from queued_tasks where id = :taskId")
        int deleteTask(@Bind("taskId") long taskId);
    }
}
