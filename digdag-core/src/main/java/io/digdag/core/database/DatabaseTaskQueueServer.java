package io.digdag.core.database;

import java.util.Collections;
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
import javax.annotation.Nullable;

import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.ErrorReporter;
import io.digdag.core.log.LogMarkers;
import io.digdag.spi.metrics.DigdagMetrics;
import static io.digdag.spi.metrics.DigdagMetrics.Category;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.core.mapper.RowMapper;
import io.digdag.spi.ImmutableTaskQueueLock;
import io.digdag.spi.TaskQueueRequest;
import io.digdag.spi.TaskQueueLock;
import io.digdag.spi.TaskQueueServer;
import io.digdag.spi.TaskConflictException;
import io.digdag.spi.TaskNotFoundException;
import io.digdag.core.repository.ResourceConflictException;
import com.google.common.annotations.VisibleForTesting;
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
    private final TransactionManager transactionManager;

    @Inject(optional = true)
    private ErrorReporter errorReporter = ErrorReporter.empty();

    @Inject
    private DigdagMetrics metrics;

    @Inject
    public DatabaseTaskQueueServer(DatabaseConfig config, TransactionManager tm, ConfigMapper cfm, DatabaseTaskQueueConfig queueConfig, ObjectMapper taskObjectMapper)
    {
        super(config.getType(), Dao.class, tm, cfm);

        this.queueConfig = queueConfig;
        this.taskObjectMapper = taskObjectMapper;
        this.transactionManager = tm;
        this.expireLockInterval = config.getExpireLockInterval();
        this.expireExecutor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("lock-expire-%d")
                .build()
                );
    }

    private final Object localTaskNoticeHelper = new Object();

    @Override
    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    public void interruptLocalWait()
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
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @PostConstruct
    public void start()
            throws Exception
    {
        expireExecutor.scheduleWithFixedDelay(
                () -> {
                    transactionManager.begin(() -> {
                        expireLocks();
                        return null;
                    });
                }, expireLockInterval, expireLockInterval, TimeUnit.SECONDS);
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

    private String statementUnixTimestampSql()
    {
        return "extract(epoch from now())";
    }

    @Override
    public void enqueueDefaultQueueTask(int siteId, TaskQueueRequest request)
        throws TaskConflictException
    {
        try {
            enqueue(siteId, null, request.getPriority(),
                    request.getUniqueName(), request.getData().orNull());
        }
        catch (ResourceConflictException ex) {
            throw new TaskConflictException(ex);
        }
    }

    @Override
    public void enqueueQueueBoundTask(int queueId, TaskQueueRequest request)
        throws TaskConflictException
    {
        try {
            // sharedAgentSiteId may be null if tasks in this queue should not be executed on shared agents.
            // TODO it should be considered to throw TaskNotFoundException
            //      if queueId doesn't exist when multi-queue is implemented
            Integer sharedAgentSiteId = autoCommit((handle, dao) -> dao.getSharedSiteId(queueId));
            enqueue(sharedAgentSiteId, queueId, request.getPriority(),
                    request.getUniqueName(), request.getData().orNull());
        }
        catch (ResourceConflictException ex) {
            throw new TaskConflictException(ex);
        }
    }

    private long enqueue(
            @Nullable Integer siteId, @Nullable Integer queueId,
            int priority, String uniqueName,
            @Nullable byte[] data)
        throws ResourceConflictException
    {
        long id = transaction((handle, dao) -> {
            long queuedTaskId = catchConflict(() ->
                dao.insertQueuedTask(siteId, queueId, uniqueName, data),
                "lock of task name=%s in site id = %d and queue id=%d", uniqueName, siteId, queueId);
            dao.insertQueuedTaskLock(queuedTaskId, siteId, queueId, priority);
            return queuedTaskId;
        }, ResourceConflictException.class);

        interruptLocalWait();

        return id;
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
        throws TaskNotFoundException, TaskConflictException
    {
        long taskLockId = parseTaskLockId(lockId);
        deleteTask0(siteId, taskLockId, agentId);
    }

    private void deleteTask0(int siteId, long taskLockId, String agentId)
        throws TaskNotFoundException, TaskConflictException
    {
        this.<Boolean, TaskNotFoundException, TaskConflictException>transaction((handle, dao) -> {
            int count;

            count = dao.deleteQueuedTask(siteId, taskLockId);
            if (count == 0) {
                throw new TaskNotFoundException("Deleting lock does not exist: lock id=" + taskLockId + " site id=" + siteId);
            }

            count = dao.deleteQueuedTaskLock(taskLockId, agentId);
            if (count == 0) {
                throw new TaskConflictException("Deleting lock does not exist or preempted by another agent: lock id=" + taskLockId + " agent id=" + agentId);
            }

            return true;
        }, TaskNotFoundException.class, TaskConflictException.class);
    }

    @Override
    public boolean forceDeleteTask(String lockId)
    {
        long taskLockId = parseTaskLockId(lockId);
        return forceDeleteTask0(taskLockId);
    }

    private boolean forceDeleteTask0(long taskLockId)
    {
        return this.transaction((handle, dao) -> {
            int taskCount = dao.forceDeleteQueuedTask(taskLockId);
            int lockCount = dao.forceDeleteQueuedTaskLock(taskLockId);
            return taskCount > 0 || lockCount > 0;
        });
    }

    public List<String> taskHeartbeat(int siteId, List<String> lockedIds, String agentId, int lockSeconds)
    {
        ImmutableList.Builder<String> notFoundList = ImmutableList.builder();
        for (String formatted : lockedIds) {
            boolean success;
            if (isSharedTaskLockId(formatted)) {
                success = taskHeartbeat0(siteId, null, parseTaskLockId(formatted), agentId, lockSeconds);
            }
            else {
                success = taskHeartbeat0(siteId, parseQueueId(formatted), parseTaskLockId(formatted), agentId, lockSeconds);
            }
            if (!success) {
                notFoundList.add(formatted);
            }
        }
        return notFoundList.build();
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
            return handle.createUpdate(
                    "update queued_task_locks" +
                    " set lock_expire_time = " + lockExpireTimeSql +
                    " where id = :id" +
                    " and lock_agent_id = :agentId" +
                    " and coalesce(site_id, (select site_id from queue_settings where id = :queueId)) = :siteId"
                )
                .bind("expireTime", Instant.now().getEpochSecond() + lockSeconds)
                .bind("id", taskLockId)
                .bind("agentId", agentId)
                .bind("queueId", queueId)
                .bind("siteId", siteId)
                .execute();
        }) > 0;
    }

    @Override
    public List<TaskQueueLock> lockSharedAgentTasks(int count, String agentId, int lockSeconds, long maxSleepMillis)
    {
        List<Integer> siteIds = autoCommit((handle, dao) -> dao.getActiveSiteIdList());
        // Here shuffles siteIds to iterate in random order so that scheduling becomes slightly more fair across sites.
        // It also improves overhead when smaller siteIds tend to have more tasks than its siteMaxConcurrency.
        Collections.shuffle(siteIds);
        for (int siteId : siteIds) {
            List<Long> taskLockIds = tryLockSharedAgentTasks(siteId, count, agentId, lockSeconds);
            if (!taskLockIds.isEmpty()) {
                ImmutableList.Builder<TaskQueueLock> builder = ImmutableList.builder();
                for (long taskLockId : taskLockIds) {
                    ImmutableTaskQueueLock data = autoCommit((handle, dao) -> dao.getTaskData(taskLockId));
                    if (data == null) {
                        // queued_task is deleted after tryLockSharedAgentTasks call.
                        // it is possible just because there are 2 different transactions.
                    }
                    else {
                        String lockId = formatSharedTaskLockId(taskLockId);
                        builder.add(data.withLockId(lockId));
                    }
                }
                return builder.build();
            }
        }

        // no tasks are ready to lock. sleep.
        if (maxSleepMillis >= 0) {
            sleepForEnqueue(maxSleepMillis);
        }
        return ImmutableList.of();
    }

    private List<Long> tryLockSharedAgentTasks(int siteId,
            int count, String agentId, int lockSeconds)
    {
        int siteMaxConcurrency = queueConfig.getSiteMaxConcurrency(siteId);

        try {
            if (!localLockMap.tryLock(siteId, 500)) {
                return ImmutableList.of();
            }
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return ImmutableList.of();
        }

        try {
            if (isEmbededDatabase()) {
                return transaction((handle, dao) -> {
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
                                "where runnings.count >= queues.max_concurrency " +
                                "and runnings.queue_id = queued_task_locks.queue_id" +
                            ") " +
                            "and not exists (" +
                              "select count(*) " +
                              "from queued_task_locks " +
                              "where lock_expire_time is not null " +
                              "and site_id = :siteId " +
                              "having count(*) >= :siteMaxConcurrency" +
                            ") " +
                            "order by queue_id, priority desc, id " +
                            "limit :limit"
                            )
                            .bind("siteId", siteId)
                            .bind("siteMaxConcurrency", siteMaxConcurrency)
                            .bind("limit", count)
                            .mapTo(long.class)
                            .list();
                    handle.createUpdate(
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

    @VisibleForTesting
    void expireLocks()
    {
        try {
            int c = autoCommit((handle, dao) -> {
                if (isEmbededDatabase()) {
                    return handle.createUpdate(
                            "update queued_task_locks" +
                                    " set lock_expire_time = NULL, lock_agent_id = NULL, retry_count = retry_count + 1" +
                                    " where lock_expire_time is not null" +
                                    " and lock_expire_time < :expireTime"
                    )
                            .bind("expireTime", Instant.now().getEpochSecond())
                            .execute();
                }
                else {
                    return handle.createUpdate(
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
            logger.error(
                    LogMarkers.UNEXPECTED_SERVER_ERROR,
                    "An uncaught exception is ignored. This lock expiration thread will be restarted.", t);
            errorReporter.reportUncaughtError(t);
            metrics.increment(Category.DB, "uncaughtErrors");
        }
    }

    static class ImmutableTaskQueueLockMapper
            implements RowMapper<ImmutableTaskQueueLock>
    {
        @Override
        public ImmutableTaskQueueLock map(ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableTaskQueueLock.builder()
                .lockId("")  // should be reset later
                .uniqueName(r.getString("unique_name"))
                .data(getOptionalBytes(r, "data"))
                .build();
        }
    }

    public interface Dao
    {
        @SqlQuery("select shared_site_id from queues where id = :queueId")
        Integer getSharedSiteId(@Bind("queueId") long queueId);

        // optimized implementation of
        //   select distinct site_id as id from queued_task_locks
        //   where lock_expire_time is null
        //   and site_id is not null
        //   order by site_id asc
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
                " (site_id, queue_id, unique_name, data, created_at)" +
                " values (:siteId, :queueId, :uniqueName, :data, now())")
        @GetGeneratedKeys
        long insertQueuedTask(@Bind("siteId") Integer siteId, @Bind("queueId") Integer queueId, @Bind("uniqueName") String uniqueName,
                @Bind("data") byte[] data);

        @SqlUpdate("insert into queued_task_locks" +
                " (id, site_id, queue_id, priority)" +
                " values (:id, :siteId, :queueId, :priority)")
        void insertQueuedTaskLock(@Bind("id") long id,
                @Bind("siteId") Integer siteId, @Bind("queueId") Integer queueId,
                @Bind("priority") int priority);

        @SqlQuery("select unique_name, data from queued_tasks where id = :taskLockId")
        ImmutableTaskQueueLock getTaskData(@Bind("taskLockId") long taskLockId);

        @SqlUpdate("delete from queued_task_locks" +
                " where id = :taskLockId" +
                " and lock_agent_id = :agentId")
        int deleteQueuedTaskLock(@Bind("taskLockId") long taskLockId, @Bind("agentId") String agentId);

        @SqlUpdate("delete from queued_task_locks" +
                " where id = :taskLockId")
        int forceDeleteQueuedTaskLock(@Bind("taskLockId") long taskLockId);

        @SqlUpdate("delete from queued_tasks" +
                " where id = :taskLockId and site_id = :siteId")
        int deleteQueuedTask(@Bind("siteId") int siteId, @Bind("taskLockId") long taskLockId);

        @SqlUpdate("delete from queued_tasks" +
                " where id = :taskLockId")
        int forceDeleteQueuedTask(@Bind("taskLockId") long taskLockId);
    }
}
