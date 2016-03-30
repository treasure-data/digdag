package io.digdag.core.database;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.time.Instant;
import java.time.ZoneId;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.session.*;
import io.digdag.core.workflow.TaskControl;
import io.digdag.spi.TaskReport;
import io.digdag.spi.TaskResult;
import io.digdag.core.workflow.TaskConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;

public class DatabaseSessionStoreManager
        extends BasicDatabaseStoreManager<DatabaseSessionStoreManager.Dao>
        implements SessionStoreManager
{
    private static final String DEFAULT_ATTEMPT_NAME = "";

    private final ObjectMapper mapper;
    private final ConfigFactory cf;
    private final ConfigMapper cfm;
    private final StoredTaskMapper stm;
    private final ArchivedTaskMapper atm;
    private final TaskAttemptSummaryMapper tasm;

    @Inject
    public DatabaseSessionStoreManager(DBI dbi, ConfigFactory cf, ConfigMapper cfm, ObjectMapper mapper, DatabaseConfig config)
    {
        super(config.getType(), Dao.class, dbi);

        dbi.registerMapper(new StoredTaskMapper(cfm));
        dbi.registerMapper(new ArchivedTaskMapper(cfm));
        dbi.registerMapper(new StoredSessionMapper(cfm));
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
        this.atm = new ArchivedTaskMapper(cfm);
        this.tasm = new TaskAttemptSummaryMapper();
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
    public StoredSessionAttemptWithSession getAttemptWithSessionById(long attemptId)
        throws ResourceNotFoundException
    {
        return requiredResource(
                (handle, dao) -> dao.getAttemptWithSessionByIdInternal(attemptId),
                "session attempt id=%d", attemptId);
    }

    @Override
    public SessionStateFlags getAttemptStateFlags(long attemptId)
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
        return SessionStateFlags.of(stateFlags);
    }

    @Override
    public boolean isAnyNotDoneAttempts()
    {
        return autoCommit((handle, dao) ->
                handle.createQuery(
                    "select count(*) from session_attempts sa" +
                    " join sessions s on s.id = sa.session_id" +
                    " where " + bitAnd("state_flags", Integer.toString(SessionStateFlags.DONE_CODE)) + " = 0"
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
        return transaction((handle, dao, ts) -> {
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
    public List<TaskStateSummary> findTasksByState(TaskStateCode state, long lastId)
    {
        return autoCommit((handle, dao) -> dao.findTasksByState(state.get(), lastId, 100));
    }

    @Override
    public List<TaskAttemptSummary> findRootTasksByStates(TaskStateCode[] states, long lastId)
    {
        return autoCommit((handle, dao) ->
                handle.createQuery(
                    "select id, attempt_id, state" +
                    " from tasks " +
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
    public boolean requestCancelAttempt(long attemptId)
    {
        return transaction((handle, dao, ts) -> {
            int n = handle.createStatement("update tasks " +
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
                        " set state_flags = " + bitOr("state_flags", Integer.toString(SessionStateFlags.CANCEL_REQUESTED_CODE)) +
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
        return transaction((handle, dao, ts) -> {
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
        return transaction((handle, dao, ts) -> {
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
        List<RuntimeException> exceptions = transaction((handle, dao, ts) -> {
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
                    " from tasks t " +
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
                    " where td.id in (" +
                        idList.stream()
                        .map(id -> Long.toString(id)).collect(Collectors.joining(", "))+")"
                )
                .map(new IdConfigMapper(cfm, "export_config", "export_params"))
                .list()
            );
        return sortConfigListByIdList(idList, list);
    }

    @Override
    public List<Config> getStoreParams(List<Long> idList)
    {
        if (idList.isEmpty()) {
            return ImmutableList.of();
        }
        List<IdConfig> list = autoCommit((handle, dao) ->
                handle.createQuery(
                    "select id, store_params" +
                    " from task_state_details" +
                    " where id in (" +
                        idList.stream()
                        .map(id -> Long.toString(id)).collect(Collectors.joining(", "))+")"
                )
                .map(new IdConfigMapper(cfm, "store_params"))
                .list()
            );
        return sortConfigListByIdList(idList, list);
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
                    " where id in (" +
                        idList.stream()
                        .map(id -> Long.toString(id)).collect(Collectors.joining(", "))+")"
                )
                .map(new IdConfigMapper(cfm, "error"))
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
                        "select t.*, td.full_name, td.local_config, td.export_config, ts.subtask_config, ts.export_params, ts.store_params, ts.error, ts.report, " +
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
            return dao.deleteTasks(attemptId);
        }

        @Override
        public boolean setDoneToAttemptState(long attemptId, boolean success)
        {
            int code = SessionStateFlags.DONE_CODE;
            if (success) {
                code |= SessionStateFlags.SUCCESS_CODE;
            }
            int n = handle.createStatement(
                    "update session_attempts" +
                    " set state_flags = " + bitOr("state_flags", Integer.toString(code)) +
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
        public long addSubtask(long attemptId, Task task)
        {
            long taskId = dao.insertTask(attemptId, task.getParentId().orNull(), task.getTaskType().get(), task.getState().get());  // tasks table don't have unique index
            dao.insertTaskDetails(taskId, task.getFullName(), task.getConfig().getLocal(), task.getConfig().getExport());
            dao.insertEmptyTaskStateDetails(taskId);
            return taskId;
        }

        @Override
        public StoredTask getTaskById(long taskId)
            throws ResourceNotFoundException
        {
            return DatabaseSessionStoreManager.this.getTaskById(handle, taskId);
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

        public boolean setDoneStateShortCircuit(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config error)
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
                        result.getStoreParams(),
                        cf.create());  // TODO create a class for stored report
                return true;
            }
            return false;
        }

        public boolean setPlannedStateWithDelayedError(long taskId, TaskStateCode beforeState, TaskStateCode afterState, int newFlags, Optional<Config> updateError)
        {
            int n = handle.createStatement("update tasks " +
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
                        " retry_at = TIMESTAMPADD('SECOND', :retryInterval, now())" +  // TODO this doesn't work on PostgreSQL
                    " where id = :id" +
                    " and state = :oldState"
                )
                .bind("id", taskId)
                .bind("oldState", beforeState.get())
                .bind("newState", afterState.get())
                .bind("stateParams", cfm.toBinding(stateParams))
                .bind("retryInterval", retryInterval)
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
                    " )")
                .bind("parentId", taskId)
                .execute();
        }

        //public boolean trySetBlockedToReadyOrShortCircuitPlanned(long taskId)
        //{
        //    int n = handle.createStatement("update tasks " +
        //            " set updated_at = now(), state = case task_type" +
        //            " when " + TaskType.GROUPING_ONLY + " then " + TaskStateCode.PLANNED_CODE +
        //            " else " + TaskStateCode.READY_CODE +
        //            " end" +
        //            " where state = " + TaskStateCode.BLOCKED_CODE +
        //            " and id = :taskId")
        //        .bind("taskId", taskId)
        //        .execute();
        //    return n > 0;
        //}
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

        @Override
        public <T> T putAndLockSession(Session session, SessionLockAction<T> func)
            throws ResourceConflictException, ResourceNotFoundException
        {
            return DatabaseSessionStoreManager.this.<T, ResourceConflictException, ResourceNotFoundException>transaction((handle, dao, ts) -> {
                long sesId;

                switch (databaseType) {
                //// This query doesn't work because ID increments when keys conflicted
                //case "h2":
                //    "merge into sessions key (repository_id, workflow_name, instant) values (DEFAULT, :repositoryId, :workflowName, :instant, NULL)";
                //    break;
                default:
                    if (!ts.isRetried()) {
                        // first try
                        try {
                            sesId = catchForeignKeyNotFound(()->
                                    catchConflict(() ->
                                        dao.insertSession(session.getRepositoryId(), session.getWorkflowName(), session.getSessionTime().getEpochSecond()),
                                        "session instant=%s in repository_id=%d and workflow_name=%s", session.getSessionTime(), session.getRepositoryId(), session.getWorkflowName()),
                                    "repository id=%d", session.getRepositoryId());
                        }
                        catch (ResourceConflictException ex) {
                            ts.retry(ex);
                            return null;
                        }
                    }
                    else {
                        StoredSession storedSession = dao.getSessionByConflictedNamesInternal(session.getRepositoryId(), session.getWorkflowName(), session.getSessionTime().getEpochSecond());
                        if (storedSession == null) {
                            throw new IllegalStateException("Database state error", ts.getLastException());
                        }
                        sesId = storedSession.getId();
                    }
                }

                StoredSession storedSession = dao.lockSession(sesId);
                if (storedSession == null) {
                    throw new IllegalStateException("Database state error");
                }

                return func.call(new DatabaseSessionControlStore(handle, siteId), storedSession);
            }, ResourceConflictException.class, ResourceNotFoundException.class);
        }

        @Override
        public List<StoredSessionAttemptWithSession> getSessions(boolean withRetriedAttempts, int pageSize, Optional<Long> lastId)
        {
            if (withRetriedAttempts) {
                return autoCommit((handle, dao) -> dao.getSessionsWithRetriedAttempts(siteId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
            else {
                return autoCommit((handle, dao) -> dao.getSessions(siteId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
        }

        @Override
        public List<StoredSessionAttemptWithSession> getSessionsOfRepository(boolean withRetriedAttempts, int repositoryId, int pageSize, Optional<Long> lastId)
        {
            if (withRetriedAttempts) {
                return autoCommit((handle, dao) -> dao.getSessionsOfRepositoryWithRetriedAttempts(siteId, repositoryId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
            else {
                return autoCommit((handle, dao) -> dao.getSessionsOfRepository(siteId, repositoryId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
        }

        @Override
        public List<StoredSessionAttemptWithSession> getSessionsOfWorkflow(boolean withRetriedAttempts, long workflowDefinitionId, int pageSize, Optional<Long> lastId)
        {
            if (withRetriedAttempts) {
                return autoCommit((handle, dao) -> dao.getSessionsOfWorkflowWithRetriedAttempts(siteId, workflowDefinitionId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
            else {
                return autoCommit((handle, dao) -> dao.getSessionsOfWorkflow(siteId, workflowDefinitionId, pageSize, lastId.or(Long.MAX_VALUE)));
            }
        }

        @Override
        public StoredSessionAttemptWithSession getSessionAttemptById(long attemptId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getSessionAttemptById(siteId, attemptId),
                    "session attempt id=%d", attemptId);
        }

        @Override
        public StoredSessionAttemptWithSession getLastSessionAttemptByNames(int repositoryId, String workflowName, Instant sessionTime)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getLastSessionAttemptByNames(siteId, repositoryId, workflowName, sessionTime.getEpochSecond()),
                    "session time=%s in repository id=%d workflow name=%s", sessionTime, repositoryId, workflowName);
        }

        @Override
        public StoredSessionAttemptWithSession getSessionAttemptByNames(int repositoryId, String workflowName, Instant sessionTime, String retryAttemptName)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getSessionAttemptByNames(siteId, repositoryId, workflowName, sessionTime.getEpochSecond(), retryAttemptName),
                    "session attempt name=%s in session repository id=%d workflow name=%s time=%s", retryAttemptName, repositoryId, workflowName, sessionTime);
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
        //public TaskStateCode getSessionStateFlags(long sesId)
        //    throws ResourceNotFoundException
        //{
        //    return TaskStateCode.of(
        //            requiredResource(
        //                dao.getSessionStateFlags(siteId, sesId),
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
                        "select t.*, td.full_name, td.local_config, td.export_config, ts.subtask_config, ts.export_params, ts.store_params, ts.error, ts.report, " +
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
        public StoredSessionAttempt insertAttempt(long sessionId, int repoId, SessionAttempt attempt)
            throws ResourceConflictException, ResourceNotFoundException
        {
            long attemptId = catchForeignKeyNotFound(() ->
                    catchConflict(() ->
                        dao.insertAttempt(siteId, repoId, sessionId,
                                attempt.getRetryAttemptName().or(DEFAULT_ATTEMPT_NAME), attempt.getWorkflowDefinitionId().orNull(),
                                SessionStateFlags.empty().get(), attempt.getTimeZone().getId(), attempt.getParams()),
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

        //public List<StoredSessionAttemptWithSession> getAllSessions()
        //{
        //    return dao.getSessions(siteId, Integer.MAX_VALUE, 0L);
        //}

        @Override
        public <T> T insertRootTask(long attemptId, Task task, SessionBuilderAction<T> func)
        {
            long taskId = dao.insertTask(attemptId, task.getParentId().orNull(), task.getTaskType().get(), task.getState().get());  // tasks table don't have unique index
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

    public interface Dao
    {
        @SqlQuery("select now() as date")
        Instant now();

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id and s.last_attempt_id = sa.id" +
                " where sa.site_id = :siteId" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getSessions(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.site_id = :siteId" +
                " and s.last_attempt_id is not null" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getSessionsWithRetriedAttempts(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id and s.last_attempt_id = sa.id" +
                " where sa.repository_id = :repoId" +
                " and sa.site_id = :siteId" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getSessionsOfRepository(@Bind("siteId") int siteId, @Bind("repoId") int repoId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.repository_id = :repoId" +
                " and sa.site_id = :siteId" +
                " and s.last_attempt_id is not null" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getSessionsOfRepositoryWithRetriedAttempts(@Bind("siteId") int siteId, @Bind("repoId") int repoId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id and s.last_attempt_id = sa.id" +
                " where sa.workflow_definition_id = :wfId" +
                " and sa.site_id = :siteId" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getSessionsOfWorkflow(@Bind("siteId") int siteId, @Bind("wfId") long wfId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.workflow_definition_id = :wfId" +
                " and sa.site_id = :siteId" +
                " and s.last_attempt_id is not null" +
                " and sa.id < :lastId" +
                " order by sa.id desc" +
                " limit :limit")
        List<StoredSessionAttemptWithSession> getSessionsOfWorkflowWithRetriedAttempts(@Bind("siteId") int siteId, @Bind("wfId") long wfId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where sa.id = :id" +
                " and sa.site_id = :siteId" +
                " and s.last_attempt_id is not null")
        StoredSessionAttemptWithSession getSessionAttemptById(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.last_attempt_id = sa.id" +
                " where s.repository_id = :repositoryId" +
                " and s.workflow_name = :workflowName" +
                " and s.session_time = :sessionTime" +
                " and sa.site_id = :siteId")
        StoredSessionAttemptWithSession getLastSessionAttemptByNames(@Bind("siteId") int siteId, @Bind("repositoryId") int repositoryId, @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime);

        @SqlQuery("select sa.*, s.session_uuid, s.workflow_name, s.session_time" +
                " from session_attempts sa" +
                " join sessions s on s.id = sa.session_id" +
                " where s.repository_id = :repositoryId" +
                " and s.workflow_name = :workflowName" +
                " and s.session_time = :sessionTime" +
                " and sa.attempt_name = :attemptName" +
                " and sa.site_id = :siteId" +
                " limit 1")
        StoredSessionAttemptWithSession getSessionAttemptByNames(@Bind("siteId") int siteId, @Bind("repositoryId") int repositoryId, @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime, @Bind("attemptName") String attemptName);

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

        @SqlQuery("select * from sessions" +
                " where id = :sessionId" +
                " for update")
        StoredSession lockSession(@Bind("sessionId") long sessionId);

        @SqlUpdate("insert into sessions (repository_id, workflow_name, session_time, last_attempt_id)" +
                " values (:repositoryId, :workflowName, :sessionTime, NULL)")
        @GetGeneratedKeys
        long insertSession(@Bind("repositoryId") int repositoryId,
                @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime);

        @SqlQuery("select * from sessions" +
                " where repository_id = :repositoryId" +
                " and workflow_name = :workflowName" +
                " and session_time = :sessionTime" +
                " limit 1")  // here allows last_attempt_id == NULL
        StoredSession getSessionByConflictedNamesInternal(@Bind("repositoryId") int repositoryId,
                @Bind("workflowName") String workflowName, @Bind("sessionTime") long sessionTime);

        @SqlUpdate("insert into session_attempts (session_id, site_id, repository_id, attempt_name, workflow_definition_id, state_flags, timezone, params, created_at)" +
                " values (:sessionId, :siteId, :repositoryId, :attemptName, :workflowDefinitionId, :stateFlags, :timezone, :params, now())")
        @GetGeneratedKeys
        long insertAttempt(@Bind("siteId") int siteId, @Bind("repositoryId") int repositoryId, @Bind("sessionId") long sessionId, @Bind("attemptName") String attemptName, @Bind("workflowDefinitionId") Long workflowDefinitionId, @Bind("stateFlags") int stateFlags, @Bind("timezone") String timezone, @Bind("params") Config params);

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
        Short getSessionStateFlags(@Bind("siteId") int siteId, @Bind("id") long sesId);

        @SqlUpdate("insert into session_monitors (attempt_id, next_run_time, type, config, created_at, updated_at)" +
                " values (:attemptId, :nextRunTime, :type, :config, now(), now())")
        @GetGeneratedKeys
        long insertSessionMonitor(@Bind("attemptId") long attemptId, @Bind("nextRunTime") long nextRunTime, @Bind("type") String type, @Bind("config") Config config);

        @SqlQuery("select id from tasks where state = :state limit :limit")
        List<Long> findAllTaskIdsByState(@Bind("state") short state, @Bind("limit") int limit);

        @SqlQuery("select id, session_id, state_flags from session_attempts where id = :attemptId for update")
        SessionAttemptSummary lockAttempt(@Bind("attemptId") long attemptId);

        @SqlUpdate("insert into tasks (attempt_id, parent_id, task_type, state, state_flags, updated_at)" +
                " values (:attemptId, :parentId, :taskType, :state, 0, now())")
        @GetGeneratedKeys
        long insertTask(@Bind("attemptId") long attemptId, @Bind("parentId") Long parentId,
                @Bind("taskType") int taskType, @Bind("state") short state);

        // TODO this should be optimized out by using TRIGGER that runs when a task is inserted
        @SqlUpdate("insert into task_details (id, full_name, local_config, export_config)" +
                " values (:id, :fullName, :localConfig, :exportConfig)")
        void insertTaskDetails(@Bind("id") long id, @Bind("fullName") String fullName, @Bind("localConfig") Config localConfig, @Bind("exportConfig") Config exportConfig);

        // TODO this should be optimized out by using TRIGGER that runs when a task is inserted
        @SqlUpdate("insert into task_state_details (id)" +
                " values (:id)")
        void insertEmptyTaskStateDetails(@Bind("id") long id);

        @SqlUpdate("insert into task_dependencies (upstream_id, downstream_id)" +
                " values (:upstreamId, :downstreamId)")
        void insertTaskDependency(@Bind("downstreamId") long downstreamId, @Bind("upstreamId") long upstreamId);

        @SqlQuery("select id, attempt_id, parent_id, state, updated_at " +
                " from tasks " +
                " where updated_at > :updatedSince" +
                " or (updated_at = :updatedSince and id > :lastId)" +
                " order by updated_at asc, id asc" +
                " limit :limit")
        List<TaskStateSummary> findRecentlyChangedTasks(@Bind("updatedSince") Instant updatedSince, @Bind("lastId") long lastId, @Bind("limit") int limit);

        @SqlQuery("select id, attempt_id, parent_id, state, updated_at " +
                " from tasks " +
                " where state = :state" +
                " and id > :lastId" +
                " order by id asc" +
                //" order by updated_at asc, id asc" +
                " limit :limit")
        List<TaskStateSummary> findTasksByState(@Bind("state") short state, @Bind("lastId") long lastId, @Bind("limit") int limit);

        @SqlQuery("select id from tasks " +
                " where id = :id" +
                " for update")
        Long lockTask(@Bind("id") long taskId);

        @SqlQuery("select id from tasks" +
                " where attempt_id = :attemptId" +  // TODO
                " and parent_id is null" +
                " for update")
        Long lockRootTask(@Bind("attemptId") long attemptId);

        @SqlUpdate("update tasks " +
                " set updated_at = now(), state = :newState" +
                " where id = :id" +
                " and state = :oldState")
        long setState(@Bind("id") long taskId, @Bind("oldState") short oldState, @Bind("newState") short newState);

        @SqlUpdate("update tasks " +
                " set updated_at = now(), state = :newState, state_params = NULL" +  // always set state_params = NULL
                " where id = :id" +
                " and state = :oldState")
        long setDoneState(@Bind("id") long taskId, @Bind("oldState") short oldState, @Bind("newState") short newState);

        @SqlUpdate("update task_state_details " +
                " set error = :error" +
                " where id = :id")
        long setError(@Bind("id") long taskId, @Bind("error") Config error);

        @SqlUpdate("update task_state_details " +
                " set subtask_config = :subtaskConfig, export_params = :exportParams, store_params = :storeParams, report = :report, error = null" +
                " where id = :id")
        long setSuccessfulReport(@Bind("id") long taskId, @Bind("subtaskConfig") Config subtaskConfig, @Bind("exportParams") Config exportParams, @Bind("storeParams") Config storeParams, @Bind("report") Config report);

        @SqlUpdate("update tasks " +
                " set updated_at = now(), retry_at = NULL, state = " + TaskStateCode.READY_CODE +
                " where state in (" + TaskStateCode.RETRY_WAITING_CODE +"," + TaskStateCode.GROUP_RETRY_WAITING_CODE + ")"+
                " and retry_at >= now()")
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
                .repositoryId(r.getInt("repository_id"))
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
                .stateFlags(SessionStateFlags.of(r.getInt("state_flags")))
                .timeZone(ZoneId.of(r.getString("timezone")))
                .params(cfm.fromResultSetOrEmpty(r, "params"))
                .createdAt(getTimestampInstant(r, "created_at"))
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
                .stateFlags(SessionStateFlags.of(r.getInt("state_flags")))
                .timeZone(ZoneId.of(r.getString("timezone")))
                .params(cfm.fromResultSetOrEmpty(r, "params"))
                .createdAt(getTimestampInstant(r, "created_at"))
                .siteId(r.getInt("site_id"))
                .sessionUuid(getUuid(r, "session_uuid"))
                .session(
                    ImmutableSession.builder()
                        .repositoryId(r.getInt("repository_id"))
                        .workflowName(r.getString("workflow_name"))
                        .sessionTime(Instant.ofEpochSecond(r.getLong("session_time")))
                        .build())
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
                .stateFlags(SessionStateFlags.of(r.getInt("state_flags")))
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
        private final ConfigMapper cfm;

        public ArchivedTaskMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public ArchivedTask map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            Config reportConfig = cfm.fromResultSetOrEmpty(r, "report");
            TaskReport report = TaskReport.builder()
                .inputs(reportConfig.getListOrEmpty("in", Config.class))
                .outputs(reportConfig.getListOrEmpty("out", Config.class))
                .build();

            return ImmutableArchivedTask.builder()
                .id(r.getLong("id"))
                .upstreams(getLongIdList(r, "upstream_ids"))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .retryAt(getOptionalTimestampInstant(r, "retry_at"))
                .stateParams(cfm.fromResultSetOrEmpty(r, "state_params"))
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

        public IdConfig(long id, Config config)
        {
            this.id = id;
            this.config = config;
        }
    }

    private static class IdConfigMapper
            implements ResultSetMapper<IdConfig>
    {
        private final ConfigMapper cfm;
        private final String configColumn;
        private final String mergeColumn;

        public IdConfigMapper(ConfigMapper cfm, String configColumn)
        {
            this(cfm, configColumn, null);
        }

        public IdConfigMapper(ConfigMapper cfm, String configColumn, String mergeColumn)
        {
            this.cfm = cfm;
            this.configColumn = configColumn;
            this.mergeColumn = mergeColumn;
        }

        @Override
        public IdConfig map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            Config config = cfm.fromResultSetOrEmpty(r, configColumn);
            if (mergeColumn != null) {
                config.merge(cfm.fromResultSetOrEmpty(r, mergeColumn));
            }
            return new IdConfig(r.getLong("id"), config);
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
}
