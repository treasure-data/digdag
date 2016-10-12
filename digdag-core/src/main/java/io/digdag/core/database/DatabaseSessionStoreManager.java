package io.digdag.core.database;

import java.util.AbstractMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.ArchivedTask;
import io.digdag.core.session.ConfigKey;
import io.digdag.core.session.ResumingTask;
import io.digdag.core.session.ImmutableArchivedTask;
import io.digdag.core.session.ImmutableResumingTask;
import io.digdag.core.session.ImmutableSession;
import io.digdag.core.session.ImmutableSessionAttemptSummary;
import io.digdag.core.session.ImmutableStoredSession;
import io.digdag.core.session.ImmutableStoredSessionAttempt;
import io.digdag.core.session.ImmutableStoredSessionAttemptWithSession;
import io.digdag.core.session.ImmutableStoredSessionMonitor;
import io.digdag.core.session.ImmutableStoredSessionWithLastAttempt;
import io.digdag.core.session.ImmutableStoredTask;
import io.digdag.core.session.ImmutableTaskAttemptSummary;
import io.digdag.core.session.ImmutableTaskRelation;
import io.digdag.core.session.ImmutableTaskStateSummary;
import io.digdag.core.session.ParameterUpdate;
import io.digdag.core.session.Session;
import io.digdag.core.session.SessionAttempt;
import io.digdag.core.session.SessionAttemptControlStore;
import io.digdag.core.session.SessionAttemptSummary;
import io.digdag.core.session.SessionControlStore;
import io.digdag.core.session.SessionMonitor;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionStoreManager;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.session.StoredSessionMonitor;
import io.digdag.core.session.StoredSessionWithLastAttempt;
import io.digdag.core.session.StoredTask;
import io.digdag.core.session.Task;
import io.digdag.core.session.TaskAttemptSummary;
import io.digdag.core.session.TaskControlStore;
import io.digdag.core.session.TaskRelation;
import io.digdag.core.session.TaskStateCode;
import io.digdag.core.session.TaskStateFlags;
import io.digdag.core.session.TaskStateSummary;
import io.digdag.core.session.TaskType;
import io.digdag.core.workflow.TaskConfig;
import io.digdag.spi.TaskReport;
import io.digdag.spi.TaskResult;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Locale.ENGLISH;

/**
 * Store session state on a database.
 *
 * Lock relations:
 *
 * Attempt initialization:
 *   // insert the root task of an attempt
 *   insertRootTask:
 *     locked session
 *
 *   // used by dynamic task generation
 *   addResumingTasks:
 *     locked root task
 *
 * Attempt execution:
 *   // generating regular dynamic tasks and monitor tasks
 *   addSubtask:
 *     locked parent task
 *
 *   // generating dynamic tasks that are resumed by previous attempt
 *   addResumedSubtask:
 *     locked parent task
 *
 *   // reinserting tasks for group-retry
 *   copyInitialTasksForRetry:
 *     locked parent task
 *     and parent task is ready
 *
 * Attempt cleanup:
 *   // SessionAttemptControlStore.archiveTasks
 *   aggregateAndInsertTaskArchive, deleteAllTasksOfAttempt, setDoneToAttemptState:
 *     locked attempt
 *     and attempt is done yet
 *
 */
public class DatabaseSessionStoreManager
        extends BasicDatabaseStoreManager<DatabaseSessionStoreManager.Dao>
        implements SessionStoreManager
{
    private static final String DEFAULT_ATTEMPT_NAME = "";

    private final ObjectMapper mapper;
    private final ConfigFactory cf;
    private final ResetKeysMapper rkm = new ResetKeysMapper();
    private final ConfigMapper cfm;
    private final StoredTaskMapper stm;
    private final ArchivedTaskMapper atm;
    private final TaskAttemptSummaryMapper tasm;

    @Inject
    public DatabaseSessionStoreManager(DBI dbi, ConfigFactory cf, ConfigMapper cfm, ObjectMapper mapper, DatabaseConfig config)
    {
        super(config.getType(), dao(config.getType()), dbi);

        dbi.registerMapper(new StoredTaskMapper(cfm));
        dbi.registerMapper(new ArchivedTaskMapper(rkm, cfm));
        dbi.registerMapper(new ResumingTaskMapper(rkm, cfm));
        dbi.registerMapper(new StoredSessionMapper(cfm));
        dbi.registerMapper(new StoredSessionWithLastAttemptMapper(cfm));
        dbi.registerMapper(new StoredSessionAttemptMapper(cfm));
        dbi.registerMapper(new StoredSessionAttemptWithSessionMapper(cfm));
        dbi.registerMapper(new TaskStateSummaryMapper());
        dbi.registerMapper(new TaskAttemptSummaryMapper());
        dbi.registerMapper(new SessionAttemptSummaryMapper());
        dbi.registerMapper(new StoredSessionMonitorMapper(cfm));
        dbi.registerMapper(new TaskRelationMapper());
        dbi.registerMapper(new InstantMapper());
        dbi.registerArgumentFactory(cfm.getArgumentFactory());

        this.mapper = mapper;
        this.cf = cf;
        this.cfm = cfm;
        this.stm = new StoredTaskMapper(cfm);
        this.atm = new ArchivedTaskMapper(rkm, cfm);
        this.tasm = new TaskAttemptSummaryMapper();
    }

    private static Class<? extends Dao> dao(String type)
    {
        switch (type) {
        case "postgresql":
            return PgDao.class;
        case "h2":
            return H2Dao.class;
        default:
            throw new IllegalArgumentException("Unknown database type: " + type);
        }
    }

    private String bitAnd(String op1, String op2)
    {
        switch (databaseType) {
        case "h2":
            return "BITAND(" + op1 + ", " + op2 + ")";
        default:
            // postgresql
            return op1 + " & " + op2;
        }
    }

    private String bitOr(String op1, String op2)
    {
        switch (databaseType) {
        case "h2":
            return "BITOR(" + op1 + ", " + op2 + ")";
        default:
            // postgresql
            return op1 + " | " + op2;
        }
    }

    private String commaGroupConcat(String column)
    {
        switch (databaseType) {
        case "h2":
            return "group_concat(" + column + " separator ',')";
        default:
            // postgresql
            return "array_to_string(array_agg(" + column + "), ',')";
        }
    }

    private String addSeconds(String timestampExpression, int seconds)
    {
        if (seconds == 0) {
            return timestampExpression;
        }
        switch (databaseType) {
        case "h2":
            return "dateadd('SECOND', " + seconds + ", " + timestampExpression + ")";
        default:
            // postgresql
            return "(" + timestampExpression + " + interval '" + seconds + " second')";
        }
    }

    private String selectTaskDetailsQuery()
    {
        return "select t.*, td.full_name, td.local_config, td.export_config, " +
                "(select " + commaGroupConcat("upstream_id") + " from task_dependencies where downstream_id = t.id) as upstream_ids" +
            " from tasks t" +
            " join session_attempts sa on sa.id = t.attempt_id" +
            " join task_details td on t.id = td.id";
    }

    @Override
    public SessionStore getSessionStore(int siteId)
    {
        return new DatabaseSessionStore(siteId);
    }

    @Override
    public Instant getStoreTime()
    {
        return autoCommit((handle, dao) -> dao.now());
    }

    @Override
    public int getSiteIdOfTask(long taskId)
        throws ResourceNotFoundException
    {
        return requiredResource(
                (handle, dao) -> dao.getSiteIdOfTask(taskId),
                "session attempt of task id=%d", taskId);
    }

    @Override
    public StoredSessionAttemptWithSession getAttemptWithSessionById(long attemptId)
        throws ResourceNotFoundException
    {
        return requiredResource(
                (handle, dao) -> dao.getAttemptWithSessionByIdInternal(attemptId),
                "session attempt id=%d", attemptId);
    }

    @Override
    public AttemptStateFlags getAttemptStateFlags(long attemptId)
        throws ResourceNotFoundException
    {
        int stateFlags = requiredResource(
                (handle, dao) ->
                    handle.createQuery(
                        "select state_flags from session_attempts sa" +
                        " join sessions s on s.id = sa.session_id" +
                        " where sa.id = :id"
                        )
                    .bind("id", attemptId)
                    .mapTo(Integer.class)
                    .first(),
                "session attempt id=%d", attemptId);
        return AttemptStateFlags.of(stateFlags);
    }

    @Override
    public boolean isAnyNotDoneAttempts()
    {
        return autoCommit((handle, dao) ->
                handle.createQuery(
                    "select count(*) from session_attempts sa" +
                    " join sessions s on s.id = sa.session_id" +
                    " where " + bitAnd("state_flags", Integer.toString(AttemptStateFlags.DONE_CODE)) + " = 0"
                    )
                .mapTo(long.class)
                .first() > 0L
            );
    }

    @Override
    public List<Long> findAllReadyTaskIds(int maxEntries)
    {
        return autoCommit((handle, dao) -> dao.findAllTaskIdsByState(TaskStateCode.READY.get(), maxEntries));
    }

    @Override
    public <T> Optional<T> lockAttemptIfExists(long attemptId, AttemptLockAction<T> func)
    {
        return transaction((handle, dao) -> {
            SessionAttemptSummary locked = dao.lockAttempt(attemptId);
            if (locked != null) {
                return Optional.of(func.call(new DatabaseSessionAttemptControlStore(handle), locked));
            }
            else {
                return Optional.<T>absent();
            }
        });
    }

    @Override
    public List<TaskStateSummary> findRecentlyChangedTasks(Instant updatedSince, long lastId)
    {
        return autoCommit((handle, dao) -> dao.findRecentlyChangedTasks(updatedSince, lastId, 100));
    }

    @Override
    public List<Long> findTasksByState(TaskStateCode state, long lastId)
    {
        return autoCommit((handle, dao) -> dao.findTasksByState(state.get(), lastId, 100));
    }

    @Override
    public List<TaskAttemptSummary> findRootTasksByStates(TaskStateCode[] states, long lastId)
    {
        return autoCommit((handle, dao) ->
                handle.createQuery(
                    "select id, attempt_id, state" +
                    " from tasks" +
                    " where parent_id is null" +
                    " and state in (" +
                        Stream.of(states)
                        .map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")" +
                    " and id > :lastId" +
                    " order by id asc" +
                    " limit :limit"
                    )
                .bind("lastId", lastId)
                .bind("limit", 100)
                .map(tasm)
                .list()
            );
    }

    @Override
    public List<Long> findDirectParentsOfBlockedTasks(long lastId)
    {
        return autoCommit((handle, dao) ->
                handle.createQuery(
                    "select distinct parent_id" +
                    " from tasks" +
                    " where parent_id > :lastId" +
                    " and state = " + TaskStateCode.BLOCKED_CODE +
                    " order by parent_id" +
                    " limit :limit"
                    )
                .bind("lastId", lastId)
                .bind("limit", 100)
                .mapTo(Long.class)
                .list()
            );
    }

    @Override
    public boolean requestCancelAttempt(long attemptId)
    {
        return transaction((handle, dao) -> {
            int n = handle.createStatement("update tasks" +
                    " set state_flags = " + bitOr("state_flags", Integer.toString(TaskStateFlags.CANCEL_REQUESTED)) +
                    " where attempt_id = :attemptId" +
                    " and state in (" +
                        Stream.of(TaskStateCode.notDoneStates())
                        .map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")"
                )
                .bind("attemptId", attemptId)
                .execute();
            if (n > 0) {
                handle.createStatement("update session_attempts" +
                        " set state_flags = " + bitOr("state_flags", Integer.toString(AttemptStateFlags.CANCEL_REQUESTED_CODE)) +
                        " where id = :attemptId")
                    .bind("attemptId", attemptId)
                    .execute();
            }
            return n > 0;
        });
    }

    @Override
    public int trySetRetryWaitingToReady()
    {
        return autoCommit((handle, dao) -> dao.trySetRetryWaitingToReady());
    }

    @Override
    public <T> Optional<T> lockTaskIfExists(long taskId, TaskLockAction<T> func)
    {
        return transaction((handle, dao) -> {
            Long locked = dao.lockTask(taskId);
            if (locked != null) {
                T result = func.call(new DatabaseTaskControlStore(handle));
                return Optional.of(result);
            }
            return Optional.<T>absent();
        });
    }

    @Override
    public <T> Optional<T> lockTaskIfExists(long taskId, TaskLockActionWithDetails<T> func)
    {
        return transaction((handle, dao) -> {
            // TODO JOIN + FOR UPDATE doesn't work with H2 database
            Long locked = dao.lockTask(taskId);
            if (locked != null) {
                try {
                    StoredTask task = getTaskById(handle, taskId);
                    T result = func.call(new DatabaseTaskControlStore(handle), task);
                    return Optional.of(result);
                }
                catch (ResourceNotFoundException ex) {
                    throw new IllegalStateException("Database state error", ex);
                }
            }
            return Optional.<T>absent();
        });
    }

    @Override
    public void lockReadySessionMonitors(Instant currentTime, SessionMonitorAction func)
    {
        List<RuntimeException> exceptions = transaction((handle, dao) -> {
            return dao.lockReadySessionMonitors(currentTime.getEpochSecond(), 10)  // TODO 10 should be configurable?
                .stream()
                .map(monitor -> {
                    try {
                        Optional<Instant> nextRunTime = func.schedule(monitor);
                        if (nextRunTime.isPresent()) {
                            dao.updateNextSessionMonitorRunTime(monitor.getId(),
                                    nextRunTime.get().getEpochSecond());
                        }
                        else {
                            dao.deleteSessionMonitor(monitor.getId());
                        }
                        return null;
                    }
                    catch (RuntimeException ex) {
                        return ex;
                    }
                })
                .filter(exception -> exception != null)
                .collect(Collectors.toList());
        });
        if (!exceptions.isEmpty()) {
            RuntimeException first = exceptions.get(0);
            for (RuntimeException ex : exceptions.subList(1, exceptions.size())) {
                first.addSuppressed(ex);
            }
            throw first;
        }
    }

    @Override
    public List<TaskRelation> getTaskRelations(long attemptId)
    {
        return autoCommit((handle, dao) ->
                handle.createQuery(
                    "select id, parent_id," +
                    " (select " + commaGroupConcat("upstream_id") + " from task_dependencies where downstream_id = t.id) as upstream_ids" +
                    " from tasks t" +
                    " where attempt_id = :attemptId"
                    )
                .bind("attemptId", attemptId)
                .map(new TaskRelationMapper())
                .list()
            );
    }

    @Override
    public List<Config> getExportParams(List<Long> idList)
    {
        if (idList.isEmpty()) {
            return ImmutableList.of();
        }
        List<IdConfig> list = autoCommit((handle, dao) ->
                handle.createQuery(
                    "select td.id, td.export_config, ts.export_params" +
                    " from task_details td" +
                    " join task_state_details ts on ts.id = td.id" +
                    " where td.id " + inLargeIdListExpression(idList)
                )
                .map(new IdConfigMapper(rkm, null, cfm, "export_config", "export_params"))
                .list()
            );
        return sortConfigListByIdList(idList, list);
    }

    @Override
    public List<ParameterUpdate> getStoreParams(List<Long> idList)
    {
        if (idList.isEmpty()) {
            return ImmutableList.of();
        }
        List<IdConfig> list = autoCommit((handle, dao) ->
                handle.createQuery(
                    "select id, store_params, reset_store_params" +
                    " from task_state_details" +
                    " where id " + inLargeIdListExpression(idList)
                )
                .map(new IdConfigMapper(rkm, "reset_store_params", cfm, "store_params", null))
                .list()
            );
        return sortParameterUpdateListByIdList(idList, list);
    }

    @Override
    public List<Config> getErrors(List<Long> idList)
    {
        if (idList.isEmpty()) {
            return ImmutableList.of();
        }
        List<IdConfig> list = autoCommit((handle, dao) ->
                handle.createQuery(
                    "select id, error" +
                    " from task_state_details" +
                    " where id " + inLargeIdListExpression(idList)
                )
                .map(new IdConfigMapper(rkm, null, cfm, "error", null))
                .list()
            );
        return sortConfigListByIdList(idList, list);
    }

    private List<Config> sortConfigListByIdList(List<Long> idList, List<IdConfig> list)
    {
        Map<Long, Config> map = new HashMap<>();
        for (IdConfig idConfig : list) {
            map.put(idConfig.id, idConfig.config);
        }
        ImmutableList.Builder<Config> builder = ImmutableList.builder();
        for (long id : idList) {
            Config config = map.get(id);
            if (config == null) {
                config = cf.create();
            }
            builder.add(config);
        }
        return builder.build();
    }

    private List<ParameterUpdate> sortParameterUpdateListByIdList(List<Long> idList, List<IdConfig> list)
    {
        Map<Long, ParameterUpdate> map = new HashMap<>();
        for (IdConfig idConfig : list) {
            map.put(idConfig.id, new ParameterUpdate(idConfig.resetKeys, idConfig.config));
        }
        ImmutableList.Builder<ParameterUpdate> builder = ImmutableList.builder();
        for (long id : idList) {
            ParameterUpdate update = map.get(id);
            if (update == null) {
                update = new ParameterUpdate(ImmutableList.of(), cf.create());
            }
            builder.add(update);
        }
        return builder.build();
    }

    private String dumpTaskArchive(List<ArchivedTask> tasks)
    {
        try {
            return mapper.writeValueAsString(tasks);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private List<ArchivedTask> loadTaskArchive(String data)
    {
        try {
            return (List<ArchivedTask>) mapper.readValue(data, mapper.getTypeFactory().constructParametrizedType(List.class, List.class, ArchivedTask.class));
        }
        catch (IOException ex) {
            throw new RuntimeException("Failed to load task archive", ex);
        }
    }

    private StoredTask getTaskById(Handle handle, long taskId)
        throws ResourceNotFoundException
    {
        return requiredResource(
            handle.createQuery(
                    selectTaskDetailsQuery() + " where t.id = :id"
                )
                .bind("id", taskId)
                .map(stm)
                .first(),
            "task id=%d", taskId);
    }

    private class DatabaseSessionAttemptControlStore
            implements SessionAttemptControlStore
    {
        private final Handle handle;
        private final Dao dao;

        public DatabaseSessionAttemptControlStore(Handle handle)
        {
            this.handle = handle;
            this.dao = handle.attach(Dao.class);
        }

        @Override
        public int aggregateAndInsertTaskArchive(long attemptId)
        {
            int count;
            String archive;

            {
                List<ArchivedTask> tasks = handle.createQuery(
                        "select t.*, td.full_name, td.local_config, td.export_config, td.resuming_task_id, ts.subtask_config, ts.export_params, ts.store_params, ts.error, ts.report, ts.reset_store_params, " +
                            "(select " + commaGroupConcat("upstream_id") + " from task_dependencies where downstream_id = t.id) as upstream_ids" +
                        " from tasks t" +
                        " join session_attempts sa on sa.id = t.attempt_id" +
                        " join task_details td on t.id = td.id" +
                        " join task_state_details ts on t.id = ts.id" +
                        " where t.attempt_id = :attemptId" +
                        " order by t.id"
                    )
                    .bind("attemptId", attemptId)
                    .map(atm)
                    .list();
                archive = dumpTaskArchive(tasks);
                count = tasks.size();
            }

            dao.insertTaskArchive(attemptId, archive);

            return count;
        }

        @Override
        public <T> T lockRootTask(long attemptId, TaskLockActionWithDetails<T> func)
            throws ResourceNotFoundException
        {
            long taskId = requiredResource(
                    dao.lockRootTask(attemptId),
                    "root task of attempt id=%d", attemptId);
            StoredTask task = getTaskById(handle, taskId);
            T result = func.call(new DatabaseTaskControlStore(handle), task);
            return result;
        }

        @Override
        public int deleteAllTasksOfAttempt(long attemptId)
        {
            dao.deleteTaskDependencies(attemptId);
            dao.deleteTaskStateDetails(attemptId);
            dao.deleteTaskDetails(attemptId);
            dao.deleteResumingTasks(attemptId);
            return dao.deleteTasks(attemptId);
        }

        @Override
        public boolean setDoneToAttemptState(long attemptId, boolean success)
        {
            int code = AttemptStateFlags.DONE_CODE;
            if (success) {
                code |= AttemptStateFlags.SUCCESS_CODE;
            }
            int n = handle.createStatement(
                    "update session_attempts" +
                    " set state_flags = " + bitOr("state_flags", Integer.toString(code)) + "," +
                    " finished_at = now()" +
                    " where id = :attemptId")
                .bind("attemptId", attemptId)
                .execute();
            return n > 0;
        }
    }

    private class DatabaseTaskControlStore
            implements TaskControlStore
    {
        private final Handle handle;
        private final Dao dao;

        public DatabaseTaskControlStore(Handle handle)
        {
            this.handle = handle;
            this.dao = handle.attach(Dao.class);
        }

        @Override
        public long getTaskCountOfAttempt(long attemptId)
        {
            long count = handle.createQuery(
                    "select count(*) from tasks t" +
                            " where t.attempt_id = :attemptId"
                    )
                    .bind("attemptId", attemptId)
                    .mapTo(long.class)
                    .first();

            return count;
        }

        @Override
        public long addSubtask(long attemptId, Task task)
        {
            long taskId = dao.insertTask(attemptId, task.getParentId().orNull(), task.getTaskType().get(), task.getState().get(), task.getStateFlags().get());  // tasks table don't have unique index
            dao.insertTaskDetails(taskId, task.getFullName(), task.getConfig().getLocal(), task.getConfig().getExport());
            dao.insertEmptyTaskStateDetails(taskId);
            return taskId;
        }

        @Override
        public long addResumedSubtask(long attemptId, long parentId,
                TaskType taskType, TaskStateCode state, TaskStateFlags flags,
                ResumingTask resumingTask)
        {
            long taskId = dao.insertResumedTask(attemptId, parentId,
                    taskType.get(),
                    state.get(),
                    flags.get(),
                    sqlTimestampOf(resumingTask.getUpdatedAt()));
            dao.insertResumedTaskDetails(taskId,
                    resumingTask.getFullName(),
                    resumingTask.getConfig().getLocal(),
                    resumingTask.getConfig().getExport(),
                    resumingTask.getSourceTaskId());
            dao.insertResumedTaskStateDetails(taskId,
                    resumingTask.getSubtaskConfig(),
                    resumingTask.getExportParams(),
                    resumingTask.getStoreParams(),
                    null,
                    resumingTask.getError());
            return taskId;
        }

        private java.sql.Timestamp sqlTimestampOf(Instant instant)
        {
            java.sql.Timestamp t = new java.sql.Timestamp(instant.getEpochSecond() * 1000);
            t.setNanos(instant.getNano());
            return t;
        }

        @Override
        public void addResumingTasks(long attemptId, List<ResumingTask> tasks)
        {
            for (ResumingTask task : tasks) {
                dao.insertResumingTask(attemptId,
                        task.getSourceTaskId(),
                        task.getFullName(),
                        sqlTimestampOf(task.getUpdatedAt()),
                        task.getConfig().getLocal(),
                        task.getConfig().getExport(),
                        task.getSubtaskConfig(),
                        task.getExportParams(),
                        rkm.toBinding(task.getResetStoreParams()),
                        task.getStoreParams(),
                        taskReportToConfig(cf, task.getReport()),
                        task.getError());
            }
        }

        @Override
        public List<ResumingTask> getResumingTasksByNamePrefix(long attemptId, String fullNamePrefix)
        {
            // TODO pattern for LIKE query needs escaping of _ and % characters
            return dao.findResumingTasksByNamePrefix(attemptId, fullNamePrefix + '%');
        }

        @Override
        public boolean copyInitialTasksForRetry(List<Long> recursiveChildrenIdList)
        {
            List<StoredTask> tasks = handle.createQuery(
                    selectTaskDetailsQuery() + " where t.id " + inLargeIdListExpression(recursiveChildrenIdList) +
                    " and " + bitAnd("t.state_flags", Integer.toString(TaskStateFlags.INITIAL_TASK)) + " != 0"  // only initial tasks
                )
                .map(stm)
                .list();
            if (tasks.isEmpty()) {
                return false;
            }
            for (StoredTask task : tasks) {
                Task newTask = Task.taskBuilder()
                    .from(task)
                    .state(TaskStateCode.BLOCKED)
                    .stateFlags(TaskStateFlags.empty())
                    .build();
                addSubtask(tasks.get(0).getAttemptId(), newTask);
            }
            return true;
        }

        @Override
        public void addDependencies(long downstream, List<Long> upstreams)
        {
            for (long upstream : upstreams) {
                dao.insertTaskDependency(downstream, upstream);  // task_dependencies table don't have unique index
            }
        }

        @Override
        public boolean isAnyProgressibleChild(long taskId)
        {
            return handle.createQuery(
                    "select id from tasks" +
                    " where parent_id = :parentId" +
                    " and (" +
                      // a child task is progressing now
                    "state in (" + Stream.of(
                            TaskStateCode.progressingStates()
                            )
                            .map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")" +
                      " or (" +
                        // or, a child task is BLOCKED and
                        "state = " + TaskStateCode.BLOCKED_CODE +
                        // it's ready to run
                        " and not exists (" +
                          "select * from tasks up" +
                          " join task_dependencies dep on up.id = dep.upstream_id" +
                          " where dep.downstream_id = tasks.id" +
                          " and up.state not in (" + Stream.of(
                                  TaskStateCode.canRunDownstreamStates()
                                  ).map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")" +
                        ")" +
                      ")" +
                    ") limit 1"
                )
                .bind("parentId", taskId)
                .mapTo(Long.class)
                .first() != null;
        }

        @Override
        public boolean isAnyErrorChild(long taskId)
        {
            return handle.createQuery(
                    "select parent_id from tasks" +
                    " where parent_id = :parentId" +
                    " and (" +
                      // a child task is progressing now
                      "state = " + TaskStateCode.ERROR.get() +
                      " or state = " + TaskStateCode.GROUP_ERROR.get() +
                    ") limit 1"
                )
                .bind("parentId", taskId)
                .mapTo(Long.class)
                .first() != null;
        }

        @Override
        public List<Config> collectChildrenErrors(long taskId)
        {
            return handle.createQuery(
                    "select ts.error from tasks t" +
                    " join task_state_details ts on t.id = ts.id" +
                    " where parent_id = :parentId" +
                    " and error is not null"
                )
                .bind("parentId", taskId)
                .map(new ConfigResultSetMapper(cfm, "error"))
                .list();
        }

        public boolean setState(long taskId, TaskStateCode beforeState, TaskStateCode afterState)
        {
            long n = dao.setState(taskId, beforeState.get(), afterState.get());
            return n > 0;
        }

        public boolean setDoneState(long taskId, TaskStateCode beforeState, TaskStateCode afterState)
        {
            long n = dao.setDoneState(taskId, beforeState.get(), afterState.get());
            return n > 0;
        }

        public boolean setErrorStateShortCircuit(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config error)
        {
            long n = dao.setDoneState(taskId, beforeState.get(), afterState.get());
            if (n > 0) {
                dao.setError(taskId, error);
                return true;
            }
            return false;
        }

        public boolean setPlannedStateSuccessful(long taskId, TaskStateCode beforeState, TaskStateCode afterState, TaskResult result)
        {
            long n = dao.setState(taskId, beforeState.get(), afterState.get());
            if (n > 0) {
                dao.setSuccessfulReport(taskId,
                        result.getSubtaskConfig(),
                        result.getExportParams(),
                        rkm.toBinding(result.getResetStoreParams()
                            .stream().map(key -> ConfigKey.parse(key))
                            .collect(Collectors.toList())),
                        result.getStoreParams(),
                        cf.create());  // TODO create a class for stored report
                return true;
            }
            return false;
        }

        public boolean setSuccessStateShortCircuit(long taskId, TaskStateCode beforeState, TaskStateCode afterState, TaskResult result)
        {
            long n = dao.setState(taskId, beforeState.get(), afterState.get());
            if (n > 0) {
                dao.setSuccessfulReport(taskId,
                        result.getSubtaskConfig(),
                        result.getExportParams(),
                        rkm.toBinding(result.getResetStoreParams()
                            .stream().map(key -> ConfigKey.parse(key))
                            .collect(Collectors.toList())),
                        result.getStoreParams(),
                        cf.create());  // TODO create a class for stored report
                return true;
            }
            return false;
        }

        public boolean setPlannedStateWithDelayedError(long taskId, TaskStateCode beforeState, TaskStateCode afterState, int newFlags, Optional<Config> updateError)
        {
            int n = handle.createStatement("update tasks" +
                    " set updated_at = now(), state = :newState, state_flags = " + bitOr("state_flags", Integer.toString(newFlags)) +
                    " where id = :id" +
                    " and state = :oldState"
                )
                .bind("id", taskId)
                .bind("oldState", beforeState.get())
                .bind("newState", afterState.get())
                .execute();
            if (n > 0) {
                if (updateError.isPresent()) {
                    dao.setError(taskId, updateError.get());
                }
                return true;
            }
            return false;
        }

        public boolean setRetryWaitingState(long taskId, TaskStateCode beforeState, TaskStateCode afterState, int retryInterval, Config stateParams, Optional<Config> updateError)
        {
            int n = handle.createStatement("update tasks" +
                    " set updated_at = now()," +
                        " state = :newState," +
                        " state_params = :stateParams," +
                        " retry_at = " + addSeconds("now()", retryInterval) + "," +
                        " retry_count = retry_count + 1" +
                    " where id = :id" +
                    " and state = :oldState"
                )
                .bind("id", taskId)
                .bind("oldState", beforeState.get())
                .bind("newState", afterState.get())
                .bind("stateParams", cfm.toBinding(stateParams))
                .execute();
            if (n > 0) {
                if (updateError.isPresent()) {
                    dao.setError(taskId, updateError.get());
                }
                return true;
            }
            return false;
        }

        public int trySetChildrenBlockedToReadyOrShortCircuitPlannedOrCanceled(long taskId)
        {
            return handle.createStatement("update tasks" +
                    " set updated_at = now(), state = case" +
                    " when task_type = " + TaskType.GROUPING_ONLY + " then " + TaskStateCode.PLANNED_CODE +
                    " when " + bitAnd("state_flags", Integer.toString(TaskStateFlags.CANCEL_REQUESTED)) + " != 0 then " + TaskStateCode.CANCELED_CODE +
                    " else " + TaskStateCode.READY_CODE +
                    " end" +
                    " where state = " + TaskStateCode.BLOCKED_CODE +
                    " and parent_id = :parentId" +
                    " and exists (" +
                      "select * from tasks pt" +
                      " where pt.id = tasks.parent_id" +
                      " and pt.state in (" + Stream.of(
                            TaskStateCode.canRunChildrenStates()
                            ).map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")" +
                    " )" +
                    " and not exists (" +
                        "select * from tasks up" +
                        " join task_dependencies dep on up.id = dep.upstream_id" +
                        " where dep.downstream_id = tasks.id" +
                        " and up.state not in (" + Stream.of(
                            TaskStateCode.canRunDownstreamStates()
                            ).map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")" +
                    ")")
                .bind("parentId", taskId)
                .execute();
        }
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

        public long getActiveAttemptCount()
        {
            return autoCommit((handle, dao) ->
                    handle.createQuery(
                        "select count(*) from session_attempts" +
                        " where site_id = :siteId" +
                        " and " + bitAnd("state_flags", Integer.toString(AttemptStateFlags.DONE_CODE)) + " = 0"
                    )
                    .bind("siteId", siteId)
                    .mapTo(long.class)
                    .first()
                );
        }

        @Override
        public <T> T putAndLockSession(Session session, SessionLockAction<T> func)
            throws ResourceConflictException, ResourceNotFoundException
        {
            return DatabaseSessionStoreManager.this.<T, ResourceConflictException, ResourceNotFoundException>transaction((handle, dao) -> {
                StoredSession storedSession;

                // select first so that conflicting insert (postgresql) or foreign key constraint violation (h2)
                // doesn't increment sequence of primary key unnecessarily
                storedSession = dao.getSessionByConflictedNamesInternal(
                        session.getProjectId(),
                        session.getWorkflowName(),
                        session.getSessionTime().getEpochSecond());

                if (storedSession == null) {
                    if (dao instanceof H2Dao) {
                        catchForeignKeyNotFound(
                            () -> {
                                ((H2Dao) dao).upsertAndLockSession(
                                    session.getProjectId(),
                                    session.getWorkflowName(),
                                    session.getSessionTime().getEpochSecond());
                                return 0;
                            },
                            "project id=%d", session.getProjectId());
                        storedSession = dao.getSessionByConflictedNamesInternal(
                                session.getProjectId(),
                                session.getWorkflowName(),
                                session.getSessionTime().getEpochSecond());
                        if (storedSession == null) {
                            throw new IllegalStateException(String.format(ENGLISH,
                                        "Database state error: locked session is null: project_id=%d, workflow_name=%s, session_time=%d",
                                        session.getProjectId(), session.getWorkflowName(), session.getSessionTime().getEpochSecond()));
                        }
                    }
                    else {
                        storedSession = catchForeignKeyNotFound(
                                () -> ((PgDao) dao).upsertAndLockSession(
                                    session.getProjectId(),
                                    session.getWorkflowName(),
                                    session.getSessionTime().getEpochSecond()),
                                "project id=%d", session.getProjectId());
                    }
                }

                return func.call(new DatabaseSessionControlStore(handle, siteId), storedSession);
            }, ResourceConflictException.class, ResourceNotFoundException.class);
        }

        @Override
        public List<StoredSessionWithLastAttempt> getSessions(int pageSize, Optional<Long> lastId)
        {
            return autoCommit((handle, dao) -> dao.getSessions(siteId, pageSize, lastId.or(Long.MAX_VALUE)));
        }

        @Override
        public StoredSessionWithLastAttempt getSessionById(long sessionId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getSession(siteId, sessionId),
                    "session id=%d", sessionId);
        }

        @Override
        public List<StoredSessionWithLastAttempt> getSessionsOfWorkflowByName(int projectId, String workflowName, int pageSize, Optional<Long> lastId)
        {
            return autoCommit((handle, dao) -> dao.getSessionsOfWorkflowByName(siteId, projectId, workflowName, pageSize, lastId.or(Long.MAX_VALUE)));
        }

        @Override
        public List<StoredSessionAttemptWithSession> getAttempts(boolean withRetriedAttempts, int pageSize, Optional<Long> lastId)
        {
            if (withRetriedAttempts) {
                return autoCommit((handle, dao) -> dao.getAttemptsWithRetries(siteId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
            else {
                return autoCommit((handle, dao) -> dao.getAttempts(siteId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
        }

        @Override
        public List<StoredSessionAttemptWithSession> getAttemptsOfProject(boolean withRetriedAttempts, int projectId, int pageSize, Optional<Long> lastId)
        {
            if (withRetriedAttempts) {
                return autoCommit((handle, dao) -> dao.getAttemptsOfProjectWithRetries(siteId, projectId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
            else {
                return autoCommit((handle, dao) -> dao.getAttemptsOfProject(siteId, projectId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
        }

        @Override
        public List<StoredSessionWithLastAttempt> getSessionsOfProject(int projectId, int pageSize, Optional<Long> lastId)
        {
            return autoCommit((handle, dao) -> dao.getSessionsOfProject(siteId, projectId, pageSize, lastId.or(Long.MAX_VALUE)));
        }

        @Override
        public List<StoredSessionAttemptWithSession> getAttemptsOfWorkflow(boolean withRetriedAttempts, long workflowDefinitionId, int pageSize, Optional<Long> lastId)
        {
            if (withRetriedAttempts) {
                return autoCommit((handle, dao) -> dao.getAttemptsOfWorkflowWithRetries(siteId, workflowDefinitionId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
            else {
                return autoCommit((handle, dao) -> dao.getAttemptsOfWorkflow(siteId, workflowDefinitionId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
        }

        @Override
        public List<StoredSessionAttempt> getAttemptsOfSession(long sessionId, int pageSize, Optional<Long> lastId)
        {
            return autoCommit((handle, dao) -> dao.getAttemptsOfSessionWithRetries(siteId, sessionId, pageSize, lastId.or(Long.MAX_VALUE)));
        }

        @Override
        public StoredSessionAttemptWithSession getAttemptById(long attemptId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getAttemptById(siteId, attemptId),
                    "session attempt id=%d", attemptId);
        }

        @Override
        public StoredSessionAttemptWithSession getLastAttemptByName(int projectId, String workflowName, Instant sessionTime)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getLastAttemptByName(siteId, projectId, workflowName, sessionTime.getEpochSecond()),
                    "session time=%s in project id=%d workflow name=%s", sessionTime, projectId, workflowName);
        }

        @Override
        public StoredSessionAttemptWithSession getAttemptByName(int projectId, String workflowName, Instant sessionTime, String retryAttemptName)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getAttemptByName(siteId, projectId, workflowName, sessionTime.getEpochSecond(), retryAttemptName),
                    "session attempt name=%s in session project id=%d workflow name=%s time=%s", retryAttemptName, projectId, workflowName, sessionTime);
        }

        @Override
        public List<StoredSessionAttemptWithSession> getOtherAttempts(long attemptId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getOtherAttempts(siteId, attemptId),
                    "session attempt id=%d", attemptId);
        }

        //@Override
        //public TaskStateCode getAttemptStateFlags(long sesId)
        //    throws ResourceNotFoundException
        //{
        //    return TaskStateCode.of(
        //            requiredResource(
        //                dao.getAttemptStateFlags(siteId, sesId),
        //                "session id=%d", sesId));
        //}

        //public List<StoredTask> getAllTasks()
        //{
        //    return handle.createQuery(
        //            selectTaskDetailsQuery() +
        //            " where sa.site_id = :siteId"
        //        )
        //        .bind("siteId", siteId)
        //        .map(stm)
        //        .list();
        //}

        @Override
        public List<ArchivedTask> getTasksOfAttempt(long attemptId)
        {
            List<ArchivedTask> tasks = autoCommit((handle, dao) ->
                    handle.createQuery(
                        "select t.*, td.full_name, td.local_config, td.export_config, td.resuming_task_id, ts.subtask_config, ts.export_params, ts.store_params, ts.error, ts.report, ts.reset_store_params, " +
                            "(select " + commaGroupConcat("upstream_id") + " from task_dependencies where downstream_id = t.id) as upstream_ids" +
                        " from tasks t" +
                        " join session_attempts sa on sa.id = t.attempt_id" +
                        " join task_details td on t.id = td.id" +
                        " join task_state_details ts on t.id = ts.id" +
                        " where sa.site_id = :siteId" +
                        " and t.attempt_id = :attemptId" +
                        " order by t.id"
                        )
                    .bind("siteId", siteId)
                    .bind("attemptId", attemptId)
                    .map(atm)
                    .list()
                );
            if (tasks.isEmpty()) {
                String archive = autoCommit((handle, dao) -> dao.getTaskArchiveById(siteId, attemptId));
                if (archive != null) {
                    return loadTaskArchive(archive);
                }
            }
            return tasks;
        }
    }

    private class DatabaseSessionControlStore
            implements SessionControlStore
    {
        private final Handle handle;
        private final int siteId;
        private final Dao dao;

        public DatabaseSessionControlStore(Handle handle, int siteId)
        {
            this.handle = handle;
            this.siteId = siteId;
            this.dao = handle.attach(Dao.class);
        }

        @Override
        public StoredSessionAttempt insertAttempt(long sessionId, int projId, SessionAttempt attempt)
            throws ResourceConflictException, ResourceNotFoundException
        {
            long attemptId = catchForeignKeyNotFound(() ->
                    catchConflict(() ->
                        dao.insertAttempt(siteId, projId, sessionId,
                                attempt.getRetryAttemptName().or(DEFAULT_ATTEMPT_NAME), attempt.getWorkflowDefinitionId().orNull(),
                                AttemptStateFlags.empty().get(), attempt.getTimeZone().getId(), attempt.getParams()),
                        "session attempt name=%s in session id=%d", attempt.getRetryAttemptName().or(DEFAULT_ATTEMPT_NAME), sessionId),
                    "workflow definition id=%d", attempt.getWorkflowDefinitionId().orNull());
            dao.updateLastAttemptId(sessionId, attemptId);
            try {
                return requiredResource(
                        dao.getAttemptByIdInternal(attemptId),
                        "attempt id=%d", attemptId);
            }
            catch (ResourceNotFoundException ex) {
                throw new IllegalStateException("Database state error", ex);
            }
        }

        @Override
        public StoredSessionAttempt getLastAttempt(long sessionId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getLastAttemptInternal(sessionId),
                    "latest attempt of session id=%d", sessionId);
        }

        @Override
        public Optional<StoredSessionAttempt> getLastAttemptIfExists(long sessionId)
        {
            return Optional.fromNullable(dao.getLastAttemptInternal(sessionId));
        }

        @Override
        public <T> T insertRootTask(long attemptId, Task task, SessionBuilderAction<T> func)
        {
            long taskId = dao.insertTask(attemptId, task.getParentId().orNull(), task.getTaskType().get(), task.getState().get(), task.getStateFlags().get());  // tasks table don't have unique index
            dao.insertTaskDetails(taskId, task.getFullName(), task.getConfig().getLocal(), task.getConfig().getExport());
            dao.insertEmptyTaskStateDetails(taskId);
            return func.call(new DatabaseTaskControlStore(handle), taskId);
        }

        @Override
        public void insertMonitors(long attemptId, List<SessionMonitor> monitors)
        {
            for (SessionMonitor monitor : monitors) {
                dao.insertSessionMonitor(attemptId, monitor.getNextRunTime().getEpochSecond(), monitor.getType(), monitor.getConfig());  // session_monitors table don't have unique index
            }
        }
    }

    public interface H2Dao
            extends Dao
    {
        // h2's MERGE doesn't reutrn generated id when conflicting row already exists
        @SqlUpdate("merge into sessions" +
                " (project_id, workflow_name, session_time)" +
                " key (project_id, workflow_name, session_time)" +
                " values (:projectId, :workflowName, :sessionTime)")
        void upsertAndLockSession(@Bind("projectId") int projectId,
                @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime);
    }

    public interface PgDao
            extends Dao
    {
        @SqlQuery("insert into sessions" +
                " (project_id, workflow_name, session_time)" +
                " values (:projectId, :workflowName, :sessionTime)" +
                " on conflict (project_id, workflow_name, session_time) do update set last_attempt_id = sessions.last_attempt_id" +
                " returning *")
                // this query includes "set last_attempt_id = sessions.last_attempt_id" because "do nothing"
                // doesn't lock the row
        StoredSession upsertAndLockSession(@Bind("projectId") int projectId,
                @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime);
    }

    public interface Dao
    {
        @SqlQuery("select now() as date")
        Instant now();

        @SqlQuery("select s.*, sa.site_id, sa.attempt_name, sa.workflow_definition_id, sa.state_flags, sa.timezone, sa.params, sa.created_at, sa.finished_at" +
                " from sessions s" +
                " join session_attempts sa on sa.id = s.last_attempt_id" +
                " where s.project_id in (select id from projects where site_id = :siteId)" +
                " and s.id < :lastId" +
                " order by s.id desc" +
                " limit :limit")
        List<StoredSessionWithLastAttempt> getSessions(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select s.*, sa.site_id, sa.attempt_name, sa.workflow_definition_id, sa.state_flags, sa.timezone, sa.params, sa.created_at, sa.finished_at" +
                " from sessions s" +
                " join session_attempts sa on sa.id = s.last_attempt_id" +
                " where s.id = :id" +
                " and sa.site_id = :siteId")
        StoredSessionWithLastAttempt getSession(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select s.*, sa.site_id, sa.attempt_name, sa.workflow_definition_id, sa.state_flags, sa.timezone, sa.params, sa.created_at, sa.finished_at" +
                " from sessions s" +
                " join session_attempts sa on sa.id = s.last_attempt_id" +
                " where s.project_id = :projId" +
                " and sa.site_id = :siteId" +
                " and s.id < :lastId" +
                " order by s.id desc" +
                " limit :limit")
        List<StoredSessionWithLastAttempt> getSessionsOfProject(@Bind("siteId") int siteId, @Bind("projId") int projId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select s.*, sa.site_id, sa.attempt_name, sa.workflow_definition_id, sa.state_flags, sa.timezone, sa.params, sa.created_at, sa.finished_at" +
                " from sessions s" +
                " join session_attempts sa on sa.id = s.last_attempt_id" +
                " where s.project_id = :projId" +
                " and s.workflow_name = :workflowName" +
                " and sa.site_id = :siteId" +
                " and s.id < :lastId" +
                " order by s.id desc" +
                " limit :limit")
        List<StoredSessionWithLastAttempt> getSessionsOfWorkflowByName(@Bind("siteId") int siteId, @Bind("projId") int projId, @Bind("workflowName") String workflowName, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.last_attempt_id = sa.id" +
                " where sa.site_id = :siteId" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getAttempts(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.site_id = :siteId" +
                " and s.last_attempt_id is not null" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getAttemptsWithRetries(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.last_attempt_id = sa.id" +
                " where sa.project_id = :projId" +
                " and sa.site_id = :siteId" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getAttemptsOfProject(@Bind("siteId") int siteId, @Bind("projId") int projId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.project_id = :projId" +
                " and sa.site_id = :siteId" +
                " and s.last_attempt_id is not null" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getAttemptsOfProjectWithRetries(@Bind("siteId") int siteId, @Bind("projId") int projId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.last_attempt_id = sa.id" +
                " where sa.workflow_definition_id = :wfId" +
                " and sa.site_id = :siteId" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getAttemptsOfWorkflow(@Bind("siteId") int siteId, @Bind("wfId") long wfId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.workflow_definition_id = :wfId" +
                " and sa.site_id = :siteId" +
                " and s.last_attempt_id is not null" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getAttemptsOfWorkflowWithRetries(@Bind("siteId") int siteId, @Bind("wfId") long wfId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select * from session_attempts" +
                " where session_id = :sessionId" +
                " and site_id = :siteId" +
                " and id < :lastId" +
                " order by id desc" +
                " limit :limit")
        List<StoredSessionAttempt> getAttemptsOfSessionWithRetries(@Bind("siteId") int siteId, @Bind("sessionId") long wfId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.id = :id" +
                " and sa.site_id = :siteId" +
                " and s.last_attempt_id is not null")
        StoredSessionAttemptWithSession getAttemptById(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.last_attempt_id = sa.id" +
                " where s.project_id = :projectId" +
                " and s.workflow_name = :workflowName" +
                " and s.session_time = :sessionTime" +
                " and sa.site_id = :siteId")
        StoredSessionAttemptWithSession getLastAttemptByName(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where s.project_id = :projectId" +
                " and s.workflow_name = :workflowName" +
                " and s.session_time = :sessionTime" +
                " and sa.attempt_name = :attemptName" +
                " and sa.site_id = :siteId" +
                " limit 1")
        StoredSessionAttemptWithSession getAttemptByName(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime, @Bind("attemptName") String attemptName);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.session_id = (" +
                    "select session_id from session_attempts" +
                    " where id = :id" +
                    " and site_id = :siteId" +
                ")" +
                " and s.last_attempt_id is not null")
        List<StoredSessionAttemptWithSession> getOtherAttempts(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select * from session_attempts sa" +
                " where id = :id limit 1")
        StoredSessionAttempt getAttemptByIdInternal(@Bind("id") long id);


        @SqlQuery("select sa.* from session_attempts sa" +
                " join sessions s on s.last_attempt_id = sa.id" +
                " where s.id = :sessionId" +
                " limit 1")
        StoredSessionAttempt getLastAttemptInternal(@Bind("sessionId") long sessionId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.id = :attemptId limit 1")
        StoredSessionAttemptWithSession getAttemptWithSessionByIdInternal(@Bind("attemptId") long attemptId);

        @SqlQuery("select site_id from tasks" +
                " join session_attempts sa on sa.id = tasks.attempt_id" +
                " where tasks.id = :taskId")
        Integer getSiteIdOfTask(@Bind("taskId") long taskId);

        @SqlQuery("select * from sessions" +
                " where project_id = :projectId" +
                " and workflow_name = :workflowName" +
                " and session_time = :sessionTime" +
                " limit 1")  // here allows last_attempt_id == NULL
        StoredSession getSessionByConflictedNamesInternal(@Bind("projectId") int projectId,
                @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime);

        @SqlUpdate("insert into session_attempts (session_id, site_id, project_id, attempt_name, workflow_definition_id, state_flags, timezone, params, created_at)" +
                " values (:sessionId, :siteId, :projectId, :attemptName, :workflowDefinitionId, :stateFlags, :timezone, :params, now())")
        @GetGeneratedKeys
        long insertAttempt(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("sessionId") long sessionId, @Bind("attemptName") String attemptName, @Bind("workflowDefinitionId") Long workflowDefinitionId, @Bind("stateFlags") int stateFlags, @Bind("timezone") String timezone, @Bind("params") Config params);

        @SqlUpdate("update sessions" +
                " set last_attempt_id = :attemptId" +
                " where id = :sessionId")
        int updateLastAttemptId(@Bind("sessionId") long sessionId, @Bind("attemptId") long attemptId);

        @SqlQuery("select state from tasks t" +
                " join sessoin_attempts a on t.attempt_id = s.id" +
                " where a.site_id = :siteId" +
                " and a.id = :id" +
                " and t.parent_id is null" +
                " limit 1")
        Short getAttemptStateFlags(@Bind("siteId") int siteId, @Bind("id") long sesId);

        @SqlUpdate("insert into session_monitors (attempt_id, next_run_time, type, config, created_at, updated_at)" +
                " values (:attemptId, :nextRunTime, :type, :config, now(), now())")
        @GetGeneratedKeys
        long insertSessionMonitor(@Bind("attemptId") long attemptId, @Bind("nextRunTime") long nextRunTime, @Bind("type") String type, @Bind("config") Config config);

        @SqlQuery("select id from tasks where state = :state limit :limit")
        List<Long> findAllTaskIdsByState(@Bind("state") short state, @Bind("limit") int limit);

        @SqlQuery("select id, session_id, state_flags from session_attempts where id = :attemptId for update")
        SessionAttemptSummary lockAttempt(@Bind("attemptId") long attemptId);

        @SqlUpdate("insert into tasks (attempt_id, parent_id, task_type, state, state_flags, updated_at)" +
                " values (:attemptId, :parentId, :taskType, :state, :stateFlags, now())")
        @GetGeneratedKeys
        long insertTask(@Bind("attemptId") long attemptId, @Bind("parentId") Long parentId,
                @Bind("taskType") int taskType, @Bind("state") short state, @Bind("stateFlags") int stateFlags);

        // TODO this should be optimized out by using TRIGGER that runs when a task is inserted
        @SqlUpdate("insert into task_details (id, full_name, local_config, export_config)" +
                " values (:id, :fullName, :localConfig, :exportConfig)")
        void insertTaskDetails(@Bind("id") long id, @Bind("fullName") String fullName, @Bind("localConfig") Config localConfig, @Bind("exportConfig") Config exportConfig);

        @SqlUpdate("insert into task_state_details (id)" +
                " values (:id)")
        void insertEmptyTaskStateDetails(@Bind("id") long id);

        @SqlUpdate("insert into task_dependencies (upstream_id, downstream_id)" +
                " values (:upstreamId, :downstreamId)")
        void insertTaskDependency(@Bind("downstreamId") long downstreamId, @Bind("upstreamId") long upstreamId);

        @SqlUpdate("insert into tasks (attempt_id, parent_id, task_type, state, state_flags, updated_at)" +
                " values (:attemptId, :parentId, :taskType, :state, :stateFlags, :updatedAt)")
        @GetGeneratedKeys
        long insertResumedTask(@Bind("attemptId") long attemptId, @Bind("parentId") long parentId, @Bind("taskType") int taskType, @Bind("state") int state, @Bind("stateFlags") int stateFlags, @Bind("updatedAt") java.sql.Timestamp updatedAt);

        @SqlUpdate("insert into task_details (id, full_name, local_config, export_config, resuming_task_id)" +
                " values (:id, :fullName, :localConfig, :exportConfig, :resumingTaskId)")
        void insertResumedTaskDetails(@Bind("id") long id, @Bind("fullName") String fullName, @Bind("localConfig") Config localConfig, @Bind("exportConfig") Config exportConfig, @Bind("resumingTaskId") long resumingTaskId);

        @SqlUpdate("insert into task_state_details (id, subtask_config, export_params, store_params, report, error)" +
                " values (:id, :subtaskConfig, :exportParams, :storeParams, :report, :error)")
        void insertResumedTaskStateDetails(@Bind("id") long id, @Bind("subtaskConfig") Config subtaskConfig, @Bind("exportParams") Config exportParams, @Bind("storeParams") Config storeParams, @Bind("report") Config report, @Bind("error") Config error);

        @SqlUpdate("insert into resuming_tasks (attempt_id, source_task_id, full_name, updated_at, local_config, export_config, subtask_config, export_params, store_params, report, error, reset_store_params)" +
                " values (:attemptId, :sourceTaskId, :fullName, :updatedAt, :localConfig, :exportConfig, :subtaskConfig, :exportParams, :storeParams, :report, :error, :reset_store_params)")
        @GetGeneratedKeys
        long insertResumingTask(
                @Bind("attemptId") long attemptId,
                @Bind("sourceTaskId") long sourceTaskId,
                @Bind("fullName") String fullName,
                @Bind("updatedAt") java.sql.Timestamp updatedAt,
                @Bind("localConfig") Config localConfig,
                @Bind("exportConfig") Config exportConfig,
                @Bind("subtaskConfig") Config subtaskConfig,
                @Bind("exportParams") Config exportParams,
                @Bind("reset_store_params") String resetStoreParams,
                @Bind("storeParams") Config storeParams,
                @Bind("report") Config report,
                @Bind("error") Config error);

        @SqlQuery("select * from resuming_tasks" +
                " where attempt_id = :attemptId" +
                " and full_name like :fullNamePattern")
        List<ResumingTask> findResumingTasksByNamePrefix(@Bind("attemptId") long attemptId, @Bind("fullNamePattern") String fullNamePattern);

        @SqlQuery("select id, attempt_id, parent_id, state, updated_at" +
                " from tasks" +
                " where updated_at > :updatedSince" +
                " or (updated_at = :updatedSince and id > :lastId)" +
                " order by updated_at asc, id asc" +
                " limit :limit")
        List<TaskStateSummary> findRecentlyChangedTasks(@Bind("updatedSince") Instant updatedSince, @Bind("lastId") long lastId, @Bind("limit") int limit);

        @SqlQuery("select id" +
                " from tasks" +
                " where state = :state" +
                " and id > :lastId" +
                " order by id asc" +
                //" order by updated_at asc, id asc" +
                " limit :limit")
        List<Long> findTasksByState(@Bind("state") short state, @Bind("lastId") long lastId, @Bind("limit") int limit);

        @SqlQuery("select id from tasks" +
                " where id = :id" +
                " for update")
        Long lockTask(@Bind("id") long taskId);

        @SqlQuery("select id from tasks" +
                " where attempt_id = :attemptId" +  // TODO
                " and parent_id is null" +
                " for update")
        Long lockRootTask(@Bind("attemptId") long attemptId);

        @SqlUpdate("update tasks" +
                " set updated_at = now(), state = :newState" +
                " where id = :id" +
                " and state = :oldState")
        long setState(@Bind("id") long taskId, @Bind("oldState") short oldState, @Bind("newState") short newState);

        @SqlUpdate("update tasks" +
                " set updated_at = now(), state = :newState, state_params = NULL" +  // always set state_params = NULL
                " where id = :id" +
                " and state = :oldState")
        long setDoneState(@Bind("id") long taskId, @Bind("oldState") short oldState, @Bind("newState") short newState);

        @SqlUpdate("update task_state_details" +
                " set error = :error" +
                " where id = :id")
        long setError(@Bind("id") long taskId, @Bind("error") Config error);

        @SqlUpdate("update task_state_details" +
                " set subtask_config = :subtaskConfig, export_params = :exportParams, store_params = :storeParams, report = :report, error = null, reset_store_params = :resetStoreParams" +
                " where id = :id")
        long setSuccessfulReport(@Bind("id") long taskId, @Bind("subtaskConfig") Config subtaskConfig, @Bind("exportParams") Config exportParams, @Bind("resetStoreParams") String resetStoreParams, @Bind("storeParams") Config storeParams, @Bind("report") Config report);

        @SqlUpdate("update tasks" +
                " set updated_at = now(), retry_at = NULL, state = " + TaskStateCode.READY_CODE +
                " where state in (" + TaskStateCode.RETRY_WAITING_CODE +"," + TaskStateCode.GROUP_RETRY_WAITING_CODE + ")" +
                " and retry_at <= now()")
        int trySetRetryWaitingToReady();

        @SqlQuery("select * from session_monitors" +
                " where next_run_time <= :currentTime" +
                " limit :limit" +
                " for update")
        List<StoredSessionMonitor> lockReadySessionMonitors(@Bind("currentTime") long currentTime, @Bind("limit") int limit);

        @SqlUpdate("update session_monitors" +
                " set next_run_time = :nextRunTime, updated_at = now()" +
                " where id = :id")
        void updateNextSessionMonitorRunTime(@Bind("id") long id, @Bind("nextRunTime") long nextRunTime);

        @SqlQuery("select tasks" +
                " from task_archives ta" +
                " join session_attempts sa on sa.id = ta.id" +
                " where sa.id = :attemptId" +
                " and sa.site_id = :siteId")
        String getTaskArchiveById(@Bind("siteId") int siteId, @Bind("attemptId") long attemptId);

        @SqlUpdate("insert into task_archives" +
                " (id, tasks, created_at)" +
                " values (:attemptId, :tasks, now())")
        void insertTaskArchive(@Bind("attemptId") long attemptId, @Bind("tasks") String tasks);

        @SqlUpdate("delete from session_monitors" +
                " where id = :id")
        void deleteSessionMonitor(@Bind("id") long id);

        @SqlUpdate("delete from tasks" +
                " where attempt_id = :attemptId")
        int deleteTasks(@Bind("attemptId") long attemptId);

        @SqlUpdate("delete from task_details" +
                " where id in (select id from tasks where attempt_id = :attemptId)")
        void deleteTaskDetails(@Bind("attemptId") long attemptId);

        @SqlUpdate("delete from task_state_details" +
                " where id in (select id from tasks where attempt_id = :attemptId)")
        void deleteTaskStateDetails(@Bind("attemptId") long attemptId);

        @SqlUpdate("delete from task_dependencies" +
                " where downstream_id in (select id from tasks where attempt_id = :attemptId)")
        void deleteTaskDependencies(@Bind("attemptId") long attemptId);

        @SqlUpdate("delete from resuming_tasks" +
                " where attempt_id = :attemptId")
        int deleteResumingTasks(@Bind("attemptId") long attemptId);
    }

    private static class InstantMapper
            implements ResultSetMapper<Instant>
    {
        @Override
        public Instant map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            java.sql.Timestamp t = r.getTimestamp("date");
            if (t == null) {
                return null;
            }
            else {
                return t.toInstant();
            }
        }
    }

    private static class StoredSessionMapper
            implements ResultSetMapper<StoredSession>
    {
        private final ConfigMapper cfm;

        public StoredSessionMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredSession map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredSession.builder()
                .id(r.getLong("id"))
                .projectId(r.getInt("project_id"))
                .workflowName(r.getString("workflow_name"))
                .sessionTime(Instant.ofEpochSecond(r.getLong("session_time")))
                .uuid(getUuid(r, "session_uuid"))
                .lastAttemptId(r.getLong("last_attempt_id"))
                .build();
        }
    }

    private static class StoredSessionAttemptMapper
            implements ResultSetMapper<StoredSessionAttempt>
    {
        private final ConfigMapper cfm;

        public StoredSessionAttemptMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredSessionAttempt map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            String attemptName = r.getString("attempt_name");
            return ImmutableStoredSessionAttempt.builder()
                .id(r.getLong("id"))
                .sessionId(r.getLong("session_id"))
                .retryAttemptName(DEFAULT_ATTEMPT_NAME.equals(attemptName) ? Optional.absent() : Optional.of(attemptName))
                .workflowDefinitionId(getOptionalLong(r, "workflow_definition_id"))
                .stateFlags(AttemptStateFlags.of(r.getInt("state_flags")))
                .timeZone(ZoneId.of(r.getString("timezone")))
                .params(cfm.fromResultSetOrEmpty(r, "params"))
                .createdAt(getTimestampInstant(r, "created_at"))
                .finishedAt(getOptionalTimestampInstant(r, "finished_at"))
                .build();
        }
    }

    private static class StoredSessionAttemptWithSessionMapper
            implements ResultSetMapper<StoredSessionAttemptWithSession>
    {
        private final ConfigMapper cfm;

        public StoredSessionAttemptWithSessionMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredSessionAttemptWithSession map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            String attemptName = r.getString("attempt_name");
            return ImmutableStoredSessionAttemptWithSession.builder()
                .id(r.getLong("id"))
                .sessionId(r.getLong("session_id"))
                .retryAttemptName(DEFAULT_ATTEMPT_NAME.equals(attemptName) ? Optional.absent() : Optional.of(attemptName))
                .workflowDefinitionId(getOptionalLong(r, "workflow_definition_id"))
                .stateFlags(AttemptStateFlags.of(r.getInt("state_flags")))
                .timeZone(ZoneId.of(r.getString("timezone")))
                .params(cfm.fromResultSetOrEmpty(r, "params"))
                .createdAt(getTimestampInstant(r, "created_at"))
                .finishedAt(getOptionalTimestampInstant(r, "finished_at"))
                .siteId(r.getInt("site_id"))
                .sessionUuid(getUuid(r, "session_uuid"))
                .session(
                    ImmutableSession.builder()
                        .projectId(r.getInt("project_id"))
                        .workflowName(r.getString("workflow_name"))
                        .sessionTime(Instant.ofEpochSecond(r.getLong("session_time")))
                        .build())
                .build();
        }
    }

    private static class StoredSessionWithLastAttemptMapper
            implements ResultSetMapper<StoredSessionWithLastAttempt>
    {
        private final ConfigMapper cfm;

        public StoredSessionWithLastAttemptMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredSessionWithLastAttempt map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            String attemptName = r.getString("attempt_name");
            return ImmutableStoredSessionWithLastAttempt.builder()
                    .id(r.getLong("id"))
                    .projectId(r.getInt("project_id"))
                    .lastAttemptId(r.getLong("last_attempt_id"))
                    .lastAttempt(ImmutableStoredSessionAttempt.builder()
                            .id(r.getLong("last_attempt_id"))
                            .retryAttemptName(DEFAULT_ATTEMPT_NAME.equals(attemptName) ? Optional.absent() : Optional.of(attemptName))
                            .workflowDefinitionId(getOptionalLong(r, "workflow_definition_id"))
                            .sessionId(r.getLong("id"))
                            .stateFlags(AttemptStateFlags.of(r.getInt("state_flags")))
                            .timeZone(ZoneId.of(r.getString("timezone")))
                            .params(cfm.fromResultSetOrEmpty(r, "params"))
                            .createdAt(getTimestampInstant(r, "created_at"))
                            .finishedAt(getOptionalTimestampInstant(r, "finished_at"))
                            .build())
                    .siteId(r.getInt("site_id"))
                    .uuid(getUuid(r, "session_uuid"))
                    .workflowName(r.getString("workflow_name"))
                    .sessionTime(Instant.ofEpochSecond(r.getLong("session_time")))
                    .build();
        }
    }

    private static class SessionAttemptSummaryMapper
            implements ResultSetMapper<SessionAttemptSummary>
    {
        @Override
        public SessionAttemptSummary map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableSessionAttemptSummary.builder()
                .id(r.getLong("id"))
                .sessionId(r.getLong("session_id"))
                .stateFlags(AttemptStateFlags.of(r.getInt("state_flags")))
                .build();
        }
    }

    private static class StoredTaskMapper
            implements ResultSetMapper<StoredTask>
    {
        private final ConfigMapper cfm;

        public StoredTaskMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredTask map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredTask.builder()
                .id(r.getLong("id"))
                .upstreams(getLongIdList(r, "upstream_ids"))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .retryAt(getOptionalTimestampInstant(r, "retry_at"))
                .stateParams(cfm.fromResultSetOrEmpty(r, "state_params"))
                .retryCount(r.getInt("retry_count"))
                .attemptId(r.getLong("attempt_id"))
                .parentId(getOptionalLong(r, "parent_id"))
                .fullName(r.getString("full_name"))
                .config(
                        TaskConfig.assumeValidated(
                                cfm.fromResultSetOrEmpty(r, "local_config"),
                                cfm.fromResultSetOrEmpty(r, "export_config")))
                .taskType(TaskType.of(r.getInt("task_type")))
                .state(TaskStateCode.of(r.getInt("state")))
                .stateFlags(TaskStateFlags.of(r.getInt("state_flags")))
                .build();
        }
    }

    private static class ArchivedTaskMapper
            implements ResultSetMapper<ArchivedTask>
    {
        private final ResetKeysMapper rkm;
        private final ConfigMapper cfm;

        public ArchivedTaskMapper(ResetKeysMapper rkm, ConfigMapper cfm)
        {
            this.rkm = rkm;
            this.cfm = cfm;
        }

        @Override
        public ArchivedTask map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            TaskReport report = taskReportFromConfig(cfm.fromResultSetOrEmpty(r, "report"));

            return ImmutableArchivedTask.builder()
                .id(r.getLong("id"))
                .upstreams(getLongIdList(r, "upstream_ids"))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .retryAt(getOptionalTimestampInstant(r, "retry_at"))
                .stateParams(cfm.fromResultSetOrEmpty(r, "state_params"))
                .retryCount(r.getInt("retry_count"))
                .attemptId(r.getLong("attempt_id"))
                .parentId(getOptionalLong(r, "parent_id"))
                .fullName(r.getString("full_name"))
                .config(
                        TaskConfig.assumeValidated(
                                cfm.fromResultSetOrEmpty(r, "local_config"),
                                cfm.fromResultSetOrEmpty(r, "export_config")))
                .taskType(TaskType.of(r.getInt("task_type")))
                .state(TaskStateCode.of(r.getInt("state")))
                .stateFlags(TaskStateFlags.of(r.getInt("state_flags")))
                .subtaskConfig(cfm.fromResultSetOrEmpty(r, "subtask_config"))
                .exportParams(cfm.fromResultSetOrEmpty(r, "export_params"))
                .resetStoreParams(rkm.fromResultSetOrEmpty(r, "reset_store_params"))
                .storeParams(cfm.fromResultSetOrEmpty(r, "store_params"))
                .report(report)
                .error(cfm.fromResultSetOrEmpty(r, "error"))
                .resumingTaskId(getOptionalLong(r, "resuming_task_id"))
                .build();
        }
    }

    private static class ResumingTaskMapper
            implements ResultSetMapper<ResumingTask>
    {
        private final ResetKeysMapper rkm;
        private final ConfigMapper cfm;

        public ResumingTaskMapper(ResetKeysMapper rkm, ConfigMapper cfm)
        {
            this.rkm = rkm;
            this.cfm = cfm;
        }

        @Override
        public ResumingTask map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            TaskReport report = taskReportFromConfig(cfm.fromResultSetOrEmpty(r, "report"));

            return ImmutableResumingTask.builder()
                .sourceTaskId(r.getLong("source_task_id"))
                .fullName(r.getString("full_name"))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .config(
                        TaskConfig.assumeValidated(
                                cfm.fromResultSetOrEmpty(r, "local_config"),
                                cfm.fromResultSetOrEmpty(r, "export_config")))
                .subtaskConfig(cfm.fromResultSetOrEmpty(r, "subtask_config"))
                .exportParams(cfm.fromResultSetOrEmpty(r, "export_params"))
                .resetStoreParams(rkm.fromResultSetOrEmpty(r, "reset_store_params"))
                .storeParams(cfm.fromResultSetOrEmpty(r, "store_params"))
                .report(report)
                .error(cfm.fromResultSetOrEmpty(r, "error"))
                .build();
        }
    }

    private static class TaskStateSummaryMapper
            implements ResultSetMapper<TaskStateSummary>
    {
        @Override
        public TaskStateSummary map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableTaskStateSummary.builder()
                .id(r.getLong("id"))
                .parentId(getOptionalLong(r, "parent_id"))
                .state(TaskStateCode.of(r.getInt("state")))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .build();
        }
    }

    private static class TaskAttemptSummaryMapper
            implements ResultSetMapper<TaskAttemptSummary>
    {
        @Override
        public TaskAttemptSummary map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableTaskAttemptSummary.builder()
                .id(r.getLong("id"))
                .attemptId(r.getLong("attempt_id"))
                .state(TaskStateCode.of(r.getInt("state")))
                .build();
        }
    }

    private static class TaskRelationMapper
            implements ResultSetMapper<TaskRelation>
    {
        @Override
        public TaskRelation map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableTaskRelation.builder()
                .id(r.getInt("id"))
                .parentId(getOptionalLong(r, "parent_id"))
                .upstreams(getLongIdList(r, "upstream_ids"))
                .build();
        }
    }

    private static class StoredSessionMonitorMapper
            implements ResultSetMapper<StoredSessionMonitor>
    {
        private final ConfigMapper cfm;

        public StoredSessionMonitorMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredSessionMonitor map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredSessionMonitor.builder()
                .id(r.getLong("id"))
                .attemptId(r.getLong("attempt_id"))
                .nextRunTime(Instant.ofEpochSecond(r.getLong("next_run_time")))
                .type(r.getString("type"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .createdAt(getTimestampInstant(r, "created_at"))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .build();
        }
    }

    private static class IdConfig
    {
        protected final long id;
        protected final Config config;
        private final List<ConfigKey> resetKeys;

        public IdConfig(long id, List<ConfigKey> resetKeys, Config config)
        {
            this.id = id;
            this.resetKeys = resetKeys;
            this.config = config;
        }
    }

    private static class IdConfigMapper
            implements ResultSetMapper<IdConfig>
    {
        private final ResetKeysMapper rkm;
        private final String resetKeysColumn;
        private final ConfigMapper cfm;
        private final String configColumn;
        private final String mergeColumn;

        public IdConfigMapper(ResetKeysMapper rkm, String resetKeysColumn, ConfigMapper cfm, String configColumn, String mergeColumn)
        {
            this.rkm = rkm;
            this.cfm = cfm;
            this.resetKeysColumn = resetKeysColumn;
            this.configColumn = configColumn;
            this.mergeColumn = mergeColumn;
        }

        @Override
        public IdConfig map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            List<ConfigKey> resetKeys = null;
            if (resetKeysColumn != null) {
                resetKeys = rkm.fromResultSetOrEmpty(r, resetKeysColumn);
            }
            Config config = cfm.fromResultSetOrEmpty(r, configColumn);
            if (mergeColumn != null) {
                config.merge(cfm.fromResultSetOrEmpty(r, mergeColumn));
            }
            return new IdConfig(r.getLong("id"), resetKeys, config);
        }
    }

    private static class ConfigResultSetMapper
            implements ResultSetMapper<Config>
    {
        private final ConfigMapper cfm;
        private final String column;

        public ConfigResultSetMapper(ConfigMapper cfm, String column)
        {
            this.cfm = cfm;
            this.column = column;
        }

        @Override
        public Config map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return cfm.fromResultSetOrEmpty(r, column);
        }
    }

    static TaskReport taskReportFromConfig(Config config)
    {
        return TaskReport.builder()
            .inputs(config.getListOrEmpty("in", Config.class))
            .outputs(config.getListOrEmpty("out", Config.class))
            .build();
    }

    static Config taskReportToConfig(ConfigFactory cf, TaskReport report)
    {
        return cf.create()
            .set("in", report.getInputs())
            .set("out", report.getOutputs())
            ;
    }
}
