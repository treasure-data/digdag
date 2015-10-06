package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.stream.Stream;
import java.util.stream.Collectors;
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
        implements SessionStoreManager, SessionStoreManager.SessionBuilderStore, TaskControlStore
{
    public static short NAMESPACE_WORKFLOW_ID = (short) 3;
    public static short NAMESPACE_REPOSITORY_ID = (short) 1;
    public static short NAMESPACE_SITE_ID = (short) 0;

    private final ConfigSourceMapper cfm;
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseSessionStoreManager(IDBI dbi, ConfigSourceMapper cfm, ObjectMapper mapper)
    {
        this.handle = dbi.open();
        this.cfm = cfm;
        JsonMapper<SessionOptions> opm = new JsonMapper<>(mapper, SessionOptions.class);
        JsonMapper<TaskReport> trm = new JsonMapper<>(mapper, TaskReport.class);
        handle.registerMapper(new StoredSessionMapper(cfm, opm));
        handle.registerMapper(new StoredTaskMapper(cfm, trm));
        handle.registerMapper(new TaskStateSummaryMapper());
        handle.registerMapper(new DateMapper());
        handle.registerArgumentFactory(cfm.getArgumentFactory());
        handle.registerArgumentFactory(opm.getArgumentFactory());
        handle.registerArgumentFactory(trm.getArgumentFactory());
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

    public List<StoredTask> getAllTasks()
    {
        return dao.findAllTasks();
    }

    public Date getStoreTime()
    {
        return dao.now();
    }

    public boolean isAnyNotDoneWorkflows()
    {
        // TODO optimize
        return handle.createQuery(
                "select count(*) from tasks" +
                " where parent_id is null" +
                " and state in (" + Stream.of(
                        TaskStateCode.notDoneStates()
                        ).map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")"
            )
            .mapTo(long.class)
            .first() > 0L;
    }

    public List<Long> findAllReadyTaskIds(int maxEntries)
    {
        return dao.findAllTaskIdsByState(TaskStateCode.READY.get(), maxEntries);
    }

    public List<TaskStateSummary> findRecentlyChangedTasks(Date updatedSince, long lastId)
    {
        return dao.findRecentlyChangedTasks(updatedSince, lastId, 100);
    }

    public List<TaskStateSummary> findTasksByState(TaskStateCode state, long lastId)
    {
        return dao.findTasksByState(state.get(), lastId, 100);
    }

    public int trySetRetryWaitingToReady()
    {
        return dao.trySetRetryWaitingToReady();
    }

    public <T> Optional<T> lockTask(long taskId, TaskLockAction<T> func)
    {
        return handle.inTransaction((handle, ses) -> {
            TaskStateSummary task = dao.lockTask(taskId);
            if (task != null) {
                TaskControl control = new TaskControl(this, task.getId(), task.getState());
                T result = func.call(control);
                return Optional.of(result);
            }
            return Optional.<T>absent();
        });
    }

    public <T> Optional<T> lockTask(long taskId, TaskLockActionWithDetails<T> func)
    {
        return handle.inTransaction((handle, ses) -> {
            // TODO JOIN + FOR UPDATE doesn't work with H2 database
            //StoredTask task = dao.lockTaskWithDetails(taskId);
            TaskStateSummary summary = dao.lockTask(taskId);
            if (summary != null) {
                StoredTask task = dao.findTaskWithDetailsById(summary.getId());
                TaskControl control = new TaskControl(this, task.getId(), task.getState());
                T result = func.call(control, task);
                return Optional.of(result);
            }
            return Optional.<T>absent();
        });
    }

    public StoredSession newSession(int siteId, Session newSession, SessionRelation relation, SessionBuilderAction func)
    {
        return handle.inTransaction((handle, ses) -> {
            long sesId;
            if (relation.getWorkflowId().isPresent()) {
                // namespace is workflow id
                sesId = dao.insertSession(siteId, NAMESPACE_WORKFLOW_ID, relation.getWorkflowId().get(), newSession.getName(), newSession.getParams(), newSession.getOptions());
                dao.insertSessionRelation(sesId, relation.getRepositoryId().get(), relation.getWorkflowId().get());
            }
            else if (relation.getRepositoryId().isPresent()) {
                // namespace is repository
                sesId = dao.insertSession(siteId, NAMESPACE_REPOSITORY_ID, relation.getRepositoryId().get(), newSession.getName(), newSession.getParams(), newSession.getOptions());
                dao.insertSessionRelation(sesId, relation.getRepositoryId().get(), null);
            }
            else {
                // namespace is site
                sesId = dao.insertSession(siteId, NAMESPACE_SITE_ID, siteId, newSession.getName(), newSession.getParams(), newSession.getOptions());
                dao.insertSessionRelation(sesId, null, null);
            }
            StoredSession session = dao.getSessionById(siteId, sesId);
            func.call(session, this);
            return session;
        });
    }

    public <T> T addRootTask(Task task, TaskLockActionWithDetails<T> func)
    {
        long taskId = dao.insertTask(task.getSessionId(), task.getParentId().orNull(), task.getTaskType().get(), task.getState().get());
        dao.insertTaskDetails(taskId, task.getFullName(), task.getConfig());
        dao.insertEmptyTaskStateDetails(taskId);
        TaskControl control = new TaskControl(this, taskId, task.getState());
        return func.call(control, dao.findTaskWithDetailsById(taskId));
    }

    public long addSubtask(Task task)
    {
        long taskId = dao.insertTask(task.getSessionId(), task.getParentId().orNull(), task.getTaskType().get(), task.getState().get());
        dao.insertTaskDetails(taskId, task.getFullName(), task.getConfig());
        dao.insertEmptyTaskStateDetails(taskId);
        return taskId;
    }

    public void addDependencies(long downstream, List<Long> upstreams)
    {
        for (long upstream : upstreams) {
            dao.insertTaskDependency(downstream, upstream);
        }
    }

    public boolean isAllChildrenDone(long taskId)
    {
        return handle.createQuery(
                "select count(*) from tasks" +
                " where parent_id = :parentId" +
                " and state in (" + Stream.of(
                        TaskStateCode.notDoneStates()
                        ).map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")"
            )
            .bind("parentId", taskId)
            .mapTo(long.class)
            .first() == 0L;
    }

    public List<ConfigSource> collectChildrenErrors(long taskId)
    {
        return handle.createQuery(
                "select ts.error from tasks t" +
                " join task_state_details ts on t.id = ts.id" +
                " where parent_id = :parentId" +
                " and error is not null"
            )
            .bind("parentId", taskId)
            .map(new ConfigSourceResultSetMapper(cfm, "error"))
            .list();
    }

    public boolean setState(long taskId, TaskStateCode beforeState, TaskStateCode afterState)
    {
        long n = dao.setState(taskId, beforeState.get(), afterState.get());
        return n > 0;
    }

    public boolean setStateWithSuccessDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, ConfigSource stateParams, ConfigSource carryParams, TaskReport report)
    {
        long n = dao.setState(taskId, beforeState.get(), afterState.get());
        if (n > 0) {
            dao.setStateDetails(taskId, stateParams, carryParams, null, report);
            return true;
        }
        return false;
    }

    public boolean setStateWithErrorDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, ConfigSource stateParams, Optional<Integer> retryInterval, ConfigSource error)
    {
        long n;
        if (retryInterval.isPresent()) {
            n = dao.setState(taskId, beforeState.get(), afterState.get(), retryInterval.get());
        }
        else {
            n = dao.setState(taskId, beforeState.get(), afterState.get());
        }
        if (n > 0) {
            dao.setStateDetails(taskId, stateParams, null, error, null);
            return true;
        }
        return false;
    }

    public boolean setStateWithStateParamsUpdate(long taskId, TaskStateCode beforeState, TaskStateCode afterState, ConfigSource stateParams, Optional<Integer> retryInterval)
    {
        long n;
        if (retryInterval.isPresent()) {
            n = dao.setState(taskId, beforeState.get(), afterState.get(), retryInterval.get());
        }
        else {
            n = dao.setState(taskId, beforeState.get(), afterState.get());
        }
        if (n > 0) {
            dao.setStateDetails(taskId, stateParams, null, null, null);
            return true;
        }
        return false;
    }

    public int trySetChildrenBlockedToReadyOrShortCircuitPlanned(long taskId)
    {
        return handle.createStatement("update tasks " +
                " set updated_at = now(), state = case task_type" +
                " when " + TaskType.GROUPING_ONLY + " then " + TaskStateCode.PLANNED_CODE +
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
                " )")
            .bind("parentId", taskId)
            .execute();
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
            return handle.inTransaction((handle, ses) -> transaction.call());
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
    }

    public interface Dao
    {
        @SqlQuery("select now() as date")
        //Date now();
        java.sql.Timestamp now();

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

        @SqlUpdate("insert into sessions (site_id, namespace_type, namespace_id, name, params, options, created_at)" +
                " values (:siteId, :namespaceType, :namespaceId, :name, :params, :options, now())")
        @GetGeneratedKeys
        long insertSession(@Bind("siteId") int siteId,  @Bind("namespaceType") short namespaceType,
                @Bind("namespaceId") int namespaceId, @Bind("name") String name, @Bind("params") ConfigSource params, @Bind("options") SessionOptions options);

        @SqlUpdate("insert into session_relations (id, repository_id, workflow_id)" +
                " values (:id, :repositoryId, :workflowId)")
        void insertSessionRelation(@Bind("id") long id,  @Bind("repositoryId") Integer repositoryId,
                @Bind("workflowId") Integer workflowId);

        @SqlQuery("select id from tasks where state = :state limit :limit")
        List<Long> findAllTaskIdsByState(@Bind("state") short state, @Bind("limit") int limit);

        @SqlUpdate("insert into tasks (session_id, parent_id, task_type, state, updated_at)" +
                " values (:sessionId, :parentId, :taskType, :state, now())")
        @GetGeneratedKeys
        long insertTask(@Bind("sessionId") long sessionId, @Bind("parentId") Long parentId,
                @Bind("taskType") int taskType, @Bind("state") short state);

        @SqlUpdate("insert into task_details (id, full_name, config)" +
                " values (:id, :fullName, :config)")
        void insertTaskDetails(@Bind("id") long id, @Bind("fullName") String fullName, @Bind("config") ConfigSource config);

        @SqlUpdate("insert into task_state_details (id)" +
                " values (:id)")
        void insertEmptyTaskStateDetails(@Bind("id") long id);

        @SqlUpdate("insert into task_dependencies (upstream_id, downstream_id)" +
                " values (:upstreamId, :downstreamId)")
        void insertTaskDependency(@Bind("downstreamId") long downstreamId, @Bind("upstreamId") long upstreamId);

        @SqlQuery("select id, parent_id, state, updated_at " +
                " from tasks " +
                " where updated_at > :updatedSince" +
                " or (updated_at = :updatedSince and id > :lastId)" +
                " order by updated_at asc, id asc" +
                " limit :limit")
        List<TaskStateSummary> findRecentlyChangedTasks(@Bind("updatedSince") Date updatedSince, @Bind("lastId") long lastId, @Bind("limit") int limit);

        @SqlQuery("select id, parent_id, state, updated_at " +
                " from tasks " +
                " where state = :state" +
                " and id > :lastId" +
                " order by updated_at asc, id asc" +
                " limit :limit")
        List<TaskStateSummary> findTasksByState(@Bind("state") short state, @Bind("lastId") long lastId, @Bind("limit") int limit);

        @SqlQuery("select id, parent_id, state, updated_at " +
                " from tasks " +
                " where id = :id" +
                " for update")
        TaskStateSummary lockTask(@Bind("id") long taskId);

        @SqlQuery("select t.*, s.site_id, td.full_name, td.config, ts.state_params, ts.carry_params, ts.error, ts.report " +
                " from tasks t " +
                " join sessions s on s.id = t.session_id " +
                " join task_details td on t.id = td.id " +
                " join task_state_details ts on t.id = ts.id " +
                " where t.id = :id" +
                " for update")
        StoredTask lockTaskWithDetails(@Bind("id") long taskId);

        @SqlQuery("select t.*, s.site_id, td.full_name, td.config, ts.state_params, ts.carry_params, ts.error, ts.report " +
                " from tasks t " +
                " join sessions s on s.id = t.session_id " +
                " join task_details td on t.id = td.id " +
                " join task_state_details ts on t.id = ts.id ")
        List<StoredTask> findAllTasks();

        @SqlQuery("select t.*, s.site_id, td.full_name, td.config, ts.state_params, ts.carry_params, ts.error, ts.report " +
                " from tasks t " +
                " join sessions s on s.id = t.session_id " +
                " join task_details td on t.id = td.id " +
                " join task_state_details ts on t.id = ts.id " +
                " where t.id = :id")
        StoredTask findTaskWithDetailsById(@Bind("id") long taskId);

        @SqlUpdate("update tasks " +
                " set updated_at = now(), state = :newState" +
                " where id = :id" +
                " and state = :oldState")
        long setState(@Bind("id") long taskId, @Bind("oldState") short oldState, @Bind("newState") short newState);

        @SqlUpdate("update tasks " +
                " set updated_at = now(), state = :newState, retry_at = now() + interval :retryInterval seconds" +
                " where id = :id" +
                " and state = :oldState")
        long setState(@Bind("id") long taskId, @Bind("oldState") short oldState, @Bind("newState") short newState, @Bind("retryInterval") int retryInterval);

        @SqlUpdate("update task_state_details " +
                " set state_params = :stateParams, carry_params = :carryParams, error = :error, report = :report" +
                " where id = :id")
        long setStateDetails(@Bind("id") long taskId, @Bind("stateParams") ConfigSource stateParams, @Bind("carryParams") ConfigSource carryParams, @Bind("error") ConfigSource error, @Bind("report") TaskReport report);

        @SqlUpdate("update tasks " +
                " set updated_at = now(), state = " + TaskStateCode.READY_CODE +
                " where state in (" + TaskStateCode.RETRY_WAITING_CODE +"," + TaskStateCode.GROUP_RETRY_WAITING_CODE + ")")
        int trySetRetryWaitingToReady();
    }

    private static class DateMapper
            implements ResultSetMapper<Date>
    {
        @Override
        public Date map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return r.getTimestamp("date");
        }
    }

    private static class StoredSessionMapper
            implements ResultSetMapper<StoredSession>
    {
        private final ConfigSourceMapper cfm;
        private final JsonMapper<SessionOptions> opm;

        public StoredSessionMapper(ConfigSourceMapper cfm, JsonMapper<SessionOptions> opm)
        {
            this.cfm = cfm;
            this.opm = opm;
        }

        @Override
        public StoredSession map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredSession.builder()
                .id(r.getLong("id"))
                .siteId(r.getInt("site_id"))
                .createdAt(r.getTimestamp("created_at"))
                .name(r.getString("name"))
                .params(cfm.fromResultSetOrEmpty(r, "params"))
                .options(opm.fromResultSet(r, "options"))
                .build();
        }
    }

    private static class StoredTaskMapper
            implements ResultSetMapper<StoredTask>
    {
        private final ConfigSourceMapper cfm;
        private final JsonMapper<TaskReport> trm;

        public StoredTaskMapper(ConfigSourceMapper cfm, JsonMapper<TaskReport> trm)
        {
            this.cfm = cfm;
            this.trm = trm;
        }

        @Override
        public StoredTask map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredTask.builder()
                .id(r.getLong("id"))
                .siteId(r.getInt("site_id"))
                // TODO upstreams
                .updatedAt(r.getTimestamp("updated_at"))
                .retryAt(getOptionalDate(r, "retry_at"))
                .stateParams(cfm.fromResultSetOrEmpty(r, "state_params"))
                .carryParams(cfm.fromResultSetOrEmpty(r, "carry_params"))
                .report(trm.fromNullableResultSet(r, "report"))
                .error(cfm.fromResultSet(r, "error"))
                .sessionId(r.getLong("session_id"))
                .parentId(getOptionalLong(r, "parent_id"))
                .fullName(r.getString("full_name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .taskType(TaskType.of(r.getInt("task_type")))
                .state(TaskStateCode.of(r.getInt("state")))
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
                .updatedAt(r.getTimestamp("updated_at"))
                .build();
        }
    }

    private static class ConfigSourceResultSetMapper
            implements ResultSetMapper<ConfigSource>
    {
        private final ConfigSourceMapper cfm;
        private final String column;

        public ConfigSourceResultSetMapper(ConfigSourceMapper cfm, String column)
        {
            this.cfm = cfm;
            this.column = column;
        }

        @Override
        public ConfigSource map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return cfm.fromResultSetOrEmpty(r, column);
        }
    }
}
