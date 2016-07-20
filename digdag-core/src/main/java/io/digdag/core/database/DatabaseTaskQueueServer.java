package io.digdag.core.database;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.queue.QueueSettingStore;
import io.digdag.core.queue.QueueSettingStoreManager;
import io.digdag.core.queue.StoredQueueSetting;
import io.digdag.core.queue.ImmutableStoredQueueSetting;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskQueueServer;
import io.digdag.spi.TaskStateException;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DatabaseTaskQueueServer
        extends BasicDatabaseStoreManager<DatabaseTaskQueueServer.Dao>
        implements TaskQueueServer
{
    private final DatabaseTaskQueueConfig queueConfig;
    private final ObjectMapper taskObjectMapper;

    private final int expireLockInterval;
    private final LocalLockMap localLockMap = new LocalLockMap();
    private final ScheduledExecutorService expireExecutor;

    private final boolean skipLockedAvailable;

    @Inject
    public DatabaseTaskQueueServer(DBI dbi, DatabaseConfig config, DatabaseTaskQueueConfig queueConfig, ObjectMapper taskObjectMapper)
    {
        super(config.getType(), Dao.class, dbi);
        this.queueConfig = queueConfig;
        this.taskObjectMapper = taskObjectMapper;
        this.expireLockInterval = config.getExpireLockInterval();
        this.expireExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("lock-expire-%d")
                .build()
                );
        this.skipLockedAvailable = checkSkipLockedAvailable(dbi);
    }

    private final Object localTaskNoticeHelper = new Object();

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    private void noticeEnqueue()
    {
        synchronized (localTaskNoticeHelper) {
            localTaskNoticeHelper.notifyAll();
        }
    }

    private void sleepForEnqueue(long maxSleepMillis)
    {
        synchronized (localTaskNoticeHelper) {
            try {
                localTaskNoticeHelper.wait(maxSleepMillis);
            }
            catch (InterruptedException ex) {
                return;
            }
        }
    }

    @PostConstruct
    public void start()
    {
        expireExecutor.scheduleWithFixedDelay(() -> expireLocks(),
                expireLockInterval, expireLockInterval, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        expireExecutor.shutdown();
        // TODO wait for shutdown completion?
    }

    private boolean isEmbededDatabase()
    {
        switch (databaseType) {
        case "h2":
            return true;
        default:
            return false;
        }
    }

    private boolean checkSkipLockedAvailable(DBI dbi)
    {
        if (isEmbededDatabase()) {
            return false;
        }
        try (Handle h = dbi.open()) {
            try {
                // added since postgresql 9.5
                h.createQuery("select 1 for update skip locked").list();
                return true;
            }
            catch (UnableToExecuteStatementException ex) {
                return false;
            }
        }
    }

    private String statementUnixTimestampSql()
    {
        return "extract(epoch from now())";
    }

    @Override
    public void enqueueSharedTask(TaskRequest request)
        throws TaskStateException
    {
        try {
            enqueue(request.getSiteId(), null,
                    request.getPriority(), request.getTaskId(),
                    encodeTaskObject(request));
        }
        catch (ResourceConflictException ex) {
            throw new TaskStateException(ex);
        }
    }

    @Override
    public void enqueueQueueBoundTask(int queueId, TaskRequest request)
        throws TaskStateException
    {
        try {
            enqueue(null, queueId,
                    request.getPriority(), request.getTaskId(),
                    encodeTaskObject(request));
        }
        catch (ResourceConflictException ex) {
            throw new TaskStateException(ex);
        }
    }

    private long enqueue(Integer siteId, Integer queueId,
            int priority, Long uniqueTaskId,
            byte[] data)
        throws ResourceConflictException
    {
        long id = transaction((handle, dao, ts) -> {
            long queuedTaskId = catchConflict(() ->
                dao.insertQueuedTask(siteId, queueId, uniqueTaskId, data),
                "lock of task id=%d in site id = %d and queue id=%d", uniqueTaskId, siteId, queueId);
            dao.insertQueuedTaskLock(queuedTaskId, siteId, queueId, priority);
            return queuedTaskId;
        }, ResourceConflictException.class);

        noticeEnqueue();

        return id;
    }

    private byte[] encodeTaskObject(TaskRequest request)
    {
        try {
            return taskObjectMapper.writeValueAsBytes(request);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private TaskRequest decodeLockedTaskObject(byte[] data, String lockId)
    {
        try {
            return TaskRequest.withLockId(
                taskObjectMapper.readValue(data, TaskRequest.class),
                lockId);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private static String formatSharedTaskLockId(long taskLockId)
    {
        return "s" + Long.toString(taskLockId);
    }

    private static String formatQueueBoundTaskLockId(long taskLockId, int queueId)
    {
        return "q" + Long.toString(taskLockId) + "." + Integer.toString(queueId);
    }

    private static boolean isSharedTaskLockId(String formatted)
    {
        return formatted.startsWith("s");
    }

    private static long parseTaskLockId(String formatted)
    {
        return Long.parseLong(formatted.split("\\.", 2)[0].substring(1));
    }

    private static int parseQueueId(String formatted)
    {
        String[] fragments = formatted.split("\\.");
        if (fragments.length != 2) {
            throw new IllegalArgumentException("Invalid queue-bound task lock id: " + formatted);
        }
        return Integer.parseInt(fragments[1]);
    }

    @Override
    public void deleteTask(int siteId, String lockId, String agentId)
        throws TaskStateException
    {
        long taskLockId = parseTaskLockId(lockId);
        try {
            deleteTask0(siteId, taskLockId, agentId);
        }
        catch (ResourceNotFoundException | ResourceConflictException ex) {
            throw new TaskStateException(ex);
        }
    }

    private void deleteTask0(int siteId, long taskLockId, String agentId)
        throws ResourceNotFoundException, ResourceConflictException
    {
        this.<Boolean, ResourceNotFoundException, ResourceConflictException>transaction((handle, dao, ts) -> {
            int count;

            count = dao.deleteQueuedTask(siteId, taskLockId);
            if (count == 0) {
                throw new ResourceNotFoundException("Deleting lock does not exist: lock id=" + taskLockId + " site id=" + siteId);
            }

            count = dao.deleteQueuedTaskLock(taskLockId, agentId);
            if (count == 0) {
                throw new ResourceConflictException("Deleting lock does not exist or preempted by another agent: lock id=" + taskLockId + " agent id=" + agentId);
            }

            return true;
        }, ResourceNotFoundException.class, ResourceConflictException.class);
    }

    public void taskHeartbeat(int siteId, List<String> lockedIds, String agentId, int lockSeconds)
        throws TaskStateException
    {
        for (String formatted : lockedIds) {
            boolean success;
            if (isSharedTaskLockId(formatted)) {
                success = taskHeartbeat0(siteId, null, parseTaskLockId(formatted), agentId, lockSeconds);
            }
            else {
                success = taskHeartbeat0(siteId, parseQueueId(formatted), parseTaskLockId(formatted), agentId, lockSeconds);
            }
            // TODO return TaskStateException if not success
        }
    }

    private boolean taskHeartbeat0(int siteId, Integer queueId, long taskLockId, String agentId, int lockSeconds)
    {
        return autoCommit((handle, dao) -> {
            String lockExpireTimeSql;
            if (isEmbededDatabase()) {
                lockExpireTimeSql = Long.toString(Instant.now().getEpochSecond() + lockSeconds);
            }
            else {
                lockExpireTimeSql = statementUnixTimestampSql() + " + " + Integer.toString(lockSeconds);
            }
            return handle.createStatement(
                    "update queued_task_locks" +
                    " set lock_expire_time = " + lockExpireTimeSql +
                    " where id = :id" +
                    " and lock_agent_id = :agentId" +
                    " and coalesce(site_id, (select site_id from queue_settings where id = :queueId)) = :site_id"
                )
                .bind("expireTime", Instant.now().getEpochSecond() + lockSeconds)
                .bind("id", taskLockId)
                .bind("agentId", agentId)
                .bind("queueId", queueId)
                .execute();
        }) > 0;
    }

    @Override
    public List<TaskRequest> lockSharedTasks(int count, String agentId, int lockSeconds, long maxSleepMillis)
    {
        int i = 0;
        for (int siteId : autoCommit((handle, dao) -> dao.getActiveSiteIdList())) {
            List<Long> taskLockIds = tryLockSharedTasks(siteId, count, agentId, lockSeconds);
            if (!taskLockIds.isEmpty()) {
                ImmutableList.Builder<TaskRequest> builder = ImmutableList.builder();
                for (long taskLockId : taskLockIds) {
                    byte[] data = autoCommit((handle, dao) -> dao.getTaskData(taskLockId));
                    if (data == null) {
                        // queued_task is deleted after tryLockSharedTasks call.
                        // it is possible just because there are 2 different transactions.
                    }
                    else {
                        String lockId = formatSharedTaskLockId(taskLockId);
                        builder.add(decodeLockedTaskObject(data, lockId));
                    }
                }
                return builder.build();
            }
            i++;
        }

        // no tasks are ready to lock. sleep.
        if (maxSleepMillis >= 0) {
            sleepForEnqueue(maxSleepMillis);
        }
        return ImmutableList.of();
    }

    private List<Long> tryLockSharedTasks(int siteId,
            int count, String agentId, int lockSeconds)
    {
        int siteMaxConcurrency = queueConfig.getSiteMaxConcurrency(siteId);

        try {
            if (!localLockMap.tryLock(siteId, 500)) {
                return ImmutableList.of();
            }
        }
        catch (InterruptedException ex) {
            return ImmutableList.of();
        }

        try {
            if (isEmbededDatabase()) {
                return transaction((handle, dao, ts) -> {
                    List<Long> taskLockIds = handle.createQuery(
                            "select id " +
                            "from queued_task_locks " +
                            "where lock_expire_time is null " +
                            "and site_id = :siteId " +
                            "and not exists (" +
                                "select * from (" +
                                    "select queue_id, count(*) as count " +
                                    "from queued_task_locks " +
                                    "where lock_expire_time is not null " +
                                    "and site_id = :siteId " +
                                    "group by queue_id" +
                                ") runnings " +
                                "join queues on queues.id = runnings.queue_id " +
                                "where runnings.count > queues.max_concurrency " +
                                "and runnings.queue_id = queued_task_locks.queue_id" +
                            ") " +
                            "and not exists (" +
                              "select count(*) " +
                              "from queued_task_locks " +
                              "where lock_expire_time is not null " +
                              "and site_id = :siteId " +
                              "having count(*) > :siteMaxConcurrency" +
                            ") " +
                            "order by queue_id, priority desc, id " +
                            "limit :limit"
                            )
                            .bind("siteId", siteId)
                            .bind("siteMaxConcurrency", siteMaxConcurrency)
                            .bind("limit", count)
                            .mapTo(long.class)
                            .list();
                    handle.createStatement(
                            "update queued_task_locks" +
                            " set lock_expire_time = :expireTime, lock_agent_id = :agentId" +
                            " where id in (" +
                                taskLockIds.stream()
                                .map(it -> Long.toString(it)).collect(Collectors.joining(", ")) +
                            ")"
                        )
                        .bind("expireTime", Instant.now().getEpochSecond() + lockSeconds)
                        .bind("agentId", agentId)
                        .execute();
                    return taskLockIds;
                });
            }
            else {
                // see DatabaseMigrator for the definition of lock_shared_tasks function.
                return autoCommit((handle, dao) ->
                        handle.createQuery(
                            "select lock_shared_tasks(:siteId, :siteMaxConcurrency, :limit, :lockExpireSeconds, :agentId)"
                        )
                        .bind("siteId", siteId)
                        .bind("siteMaxConcurrency", siteMaxConcurrency)
                        .bind("limit", count)
                        .bind("lockExpireSeconds", lockSeconds)
                        .bind("agentId", agentId)
                        .mapTo(long.class)
                        .list()
                    );
            }
        }
        finally {
            localLockMap.unlock(siteId);
        }
    }

    private void expireLocks()
    {
        try {
            int c = autoCommit((handle, dao) -> {
                if (isEmbededDatabase()) {
                    return handle.createStatement(
                            "update queued_task_locks" +
                            " set lock_expire_time = NULL, lock_agent_id = NULL, retry_count = retry_count + 1" +
                            " where lock_expire_time is not null" +
                            " and lock_expire_time < :expireTime"
                        )
                        .bind("expireTime", Instant.now().getEpochSecond())
                        .execute();
                }
                else {
                    return handle.createStatement(
                            "update queued_task_locks" +
                            " set lock_expire_time = NULL, lock_agent_id = NULL, retry_count = retry_count + 1" +
                            " where lock_expire_time is not null" +
                            " and lock_expire_time < " + statementUnixTimestampSql()
                        )
                        .execute();
                }
            });
            if (c > 0) {
                logger.warn("{} task locks are expired. Tasks will be retried.", c);
            }
        }
        catch (Throwable t) {
            logger.error("An uncaught exception is ignored. Lock expireation thread will be retried.", t);
        }
    }

    public interface Dao
    {
        // optimized implementation of
        //   select distinct site_id as id from queued_task_locks
        //   where lock_expire_time is null
        //   and site_id is not null
        @SqlQuery(
                "with recursive t (site_id) as (" +
                    "(" +
                        "select site_id from queued_task_locks " +
                        "where lock_expire_time is null " +
                        "and site_id is not null " +
                        "order by site_id limit 1" +
                    ") " +
                    "union all " +
                    "select (" +
                        "select site_id from queued_task_locks " +
                        "where lock_expire_time is null " +
                        "and site_id is not null " +
                        "and site_id > t.site_id " +
                        "order by site_id limit 1" +
                    ") from t where t.site_id is not null" +
                ") " +
                "select site_id as id from t " +
                "where site_id is not null")
        List<Integer> getActiveSiteIdList();

        @SqlUpdate("insert into queued_tasks" +
                " (site_id, queue_id, task_id, data, created_at)" +
                " values (:siteId, :queueId, :uniqueTaskId, :data, now())")
        @GetGeneratedKeys
        long insertQueuedTask(@Bind("siteId") Integer siteId, @Bind("queueId") Integer queueId, @Bind("uniqueTaskId") long uniqueTaskId,
                @Bind("data") byte[] data);

        @SqlUpdate("insert into queued_task_locks" +
                " (id, site_id, queue_id, priority)" +
                " values (:id, :siteId, :queueId, :priority)")
        void insertQueuedTaskLock(@Bind("id") long id,
                @Bind("siteId") Integer siteId, @Bind("queueId") Integer queueId,
                @Bind("priority") int priority);

        @SqlQuery("select data from queued_tasks where id = :taskLockId")
        byte[] getTaskData(@Bind("taskLockId") long taskLockId);

        @SqlUpdate("delete from queued_task_locks" +
                " where id = :taskLockId" +
                " and lock_agent_id = :agentId")
        int deleteQueuedTaskLock(@Bind("taskLockId") long taskLockId, @Bind("agentId") String agentId);

        @SqlUpdate("delete from queued_tasks" +
                " where id = :taskLockId and site_id = :siteId")
        int deleteQueuedTask(@Bind("siteId") int siteId, @Bind("taskLockId") long taskLockId);
    }
}
