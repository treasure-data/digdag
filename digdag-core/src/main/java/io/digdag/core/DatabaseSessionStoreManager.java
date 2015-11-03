package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.Map;
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
import io.digdag.core.config.Config;

public class DatabaseSessionStoreManager
        extends BasicDatabaseStoreManager
        implements SessionStoreManager, SessionStoreManager.SessionBuilderStore, TaskControlStore
{
    public static short NAMESPACE_WORKFLOW_ID = (short) 3;
    public static short NAMESPACE_REPOSITORY_ID = (short) 1;
    public static short NAMESPACE_SITE_ID = (short) 0;

    private final StoredTaskMapper stm;
    private final ConfigResultSetMapper errorRsm;
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseSessionStoreManager(IDBI dbi, ConfigMapper cfm, ObjectMapper mapper)
    {
        this.handle = dbi.open();
        JsonMapper<SessionOptions> opm = new JsonMapper<>(mapper, SessionOptions.class);
        JsonMapper<TaskReport> trm = new JsonMapper<>(mapper, TaskReport.class);
        this.stm = new StoredTaskMapper(cfm, trm);
        this.errorRsm = new ConfigResultSetMapper(cfm, "error");
        handle.registerMapper(stm);
        handle.registerMapper(new StoredSessionMapper(cfm, opm));
        handle.registerMapper(new TaskStateSummaryMapper());
        handle.registerMapper(new StoredSessionMonitorMapper(cfm));
        handle.registerMapper(new SessionRelationMapper());
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

    private String selectTaskDetailsQuery()
    {
        return "select t.*, s.site_id, td.full_name, td.config, ts.state_params, ts.carry_params, ts.error, ts.report, " +
                "(select group_concat(upstream_id separator ',') from task_dependencies where downstream_id = t.id) as upstream_ids" +  // TODO postgresql
            " from tasks t " +
            " join sessions s on s.id = t.session_id " +
            " join task_details td on t.id = td.id " +
            " join task_state_details ts on t.id = ts.id ";
    }

    public SessionStore getSessionStore(int siteId)
    {
        return new DatabaseSessionStore(siteId);
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
            TaskStateSummary summary = dao.lockTask(taskId);
            if (summary != null) {
                StoredTask task = getTaskById(summary.getId());
                TaskControl control = new TaskControl(this, task.getId(), task.getState());
                T result = func.call(control, task);
                return Optional.of(result);
            }
            return Optional.<T>absent();
        });
    }

    public <T> Optional<T> lockRootTask(long sessionId, TaskLockActionWithDetails<T> func)
    {
        return handle.inTransaction((handle, ses) -> {
            TaskStateSummary summary = dao.lockRootTask(sessionId);
            if (summary != null) {
                StoredTask task = getTaskById(summary.getId());
                TaskControl control = new TaskControl(this, task.getId(), task.getState());
                T result = func.call(control, task);
                return Optional.of(result);
            }
            return Optional.<T>absent();
        });
    }

    public StoredSession newSession(Session newSession, SessionRelation relation, SessionBuilderAction func)
    {
        return handle.inTransaction((handle, ses) -> {
            long sesId;
            if (relation.getWorkflowId().isPresent()) {
                // namespace is workflow id
                sesId = dao.insertSession(relation.getSiteId(), NAMESPACE_WORKFLOW_ID, relation.getWorkflowId().get(), newSession.getName(), newSession.getParams(), newSession.getOptions());
                dao.insertSessionRelation(sesId, relation.getRepositoryId().get(), relation.getWorkflowId().get());
            }
            else if (relation.getRepositoryId().isPresent()) {
                // namespace is repository
                sesId = dao.insertSession(relation.getSiteId(), NAMESPACE_REPOSITORY_ID, relation.getRepositoryId().get(), newSession.getName(), newSession.getParams(), newSession.getOptions());
                dao.insertSessionRelation(sesId, relation.getRepositoryId().get(), null);
            }
            else {
                // namespace is site
                sesId = dao.insertSession(relation.getSiteId(), NAMESPACE_SITE_ID, relation.getSiteId(), newSession.getName(), newSession.getParams(), newSession.getOptions());
                dao.insertSessionRelation(sesId, null, null);
            }
            StoredSession session = dao.getSessionById(relation.getSiteId(), sesId);
            func.call(session, this);
            return session;
        });
    }

    public SessionRelation getSessionRelationById(long sessionId)
    {
        return dao.getSessionRelationById(sessionId);
    }

    public <T> T addRootTask(Task task, TaskLockActionWithDetails<T> func)
    {
        long taskId = dao.insertTask(task.getSessionId(), task.getParentId().orNull(), task.getTaskType().get(), task.getState().get());
        dao.insertTaskDetails(taskId, task.getFullName(), task.getConfig());
        dao.insertEmptyTaskStateDetails(taskId);
        TaskControl control = new TaskControl(this, taskId, task.getState());
        return func.call(control, getTaskById(taskId));
    }

    public void addMonitors(long sessionId, List<SessionMonitor> monitors)
    {
        for (SessionMonitor monitor : monitors) {
            dao.insertSessionMonitor(sessionId, monitor.getConfig(), monitor.getNextRunTime().getTime() / 1000);
        }
    }

    public long addSubtask(Task task)
    {
        long taskId = dao.insertTask(task.getSessionId(), task.getParentId().orNull(), task.getTaskType().get(), task.getState().get());
        dao.insertTaskDetails(taskId, task.getFullName(), task.getConfig());
        dao.insertEmptyTaskStateDetails(taskId);
        return taskId;
    }

    public StoredTask getTaskById(long taskId)
    {
        return handle.createQuery(
                selectTaskDetailsQuery() + " where t.id = :id"
            )
            .bind("id", taskId)
            .map(stm)
            .first();
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
                " and (" +
                  // task is not done and not BLOCKED
                  "state in (" + Stream.of(
                          TaskStateCode.notDoneStates()
                          )
                        .filter(it -> it != TaskStateCode.BLOCKED)
                        .map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")" +
                  " or (" +
                    // or, task is BLOCKED and
                    " state = " + TaskStateCode.BLOCKED_CODE +
                    // upstream tasks are runnable
                    " and not exists (" +
                      " select * from tasks up" +
                      " join task_dependencies dep on up.id = dep.upstream_id" +
                      " where dep.downstream_id = tasks.id" +
                      " and up.state not in (" + Stream.of(
                              TaskStateCode.canRunDownstreamStates()
                              ).map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")" +
                      " and up.state in (" + Stream.of(
                              TaskStateCode.doneStates()
                              ).map(it -> Short.toString(it.get())).collect(Collectors.joining(", ")) + ")" +
                    " )" +
                  ")" +
                ")"
            )
            .bind("parentId", taskId)
            .mapTo(long.class)
            .first() == 0L;
    }

    public List<Config> collectChildrenErrors(long taskId)
    {
        return handle.createQuery(
                "select ts.error from tasks t" +
                " join task_state_details ts on t.id = ts.id" +
                " where parent_id = :parentId" +
                " and error is not null"
            )
            .bind("parentId", taskId)
            .map(errorRsm)
            .list();
    }

    public boolean setState(long taskId, TaskStateCode beforeState, TaskStateCode afterState)
    {
        long n = dao.setState(taskId, beforeState.get(), afterState.get());
        return n > 0;
    }

    public boolean setStateWithSuccessDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, TaskReport report)
    {
        long n = dao.setState(taskId, beforeState.get(), afterState.get());
        if (n > 0) {
            dao.setStateDetails(taskId, stateParams, report.getCarryParams(), null,
                    // TODO create a class for stored report
                    report.getCarryParams().getFactory().create()
                        .set("in", report.getInputs())
                        .set("out", report.getOutputs()));
            return true;
        }
        return false;
    }

    public boolean setStateWithErrorDetails(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, Optional<Integer> retryInterval, Config error)
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

    public boolean setStateWithStateParamsUpdate(long taskId, TaskStateCode beforeState, TaskStateCode afterState, Config stateParams, Optional<Integer> retryInterval)
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

    public void lockReadySessionMonitors(Date currentTime, SessionMonitorAction func)
    {
        List<RuntimeException> exceptions = handle.inTransaction((handle, session) -> {
            return dao.lockReadySessionMonitors(currentTime.getTime() / 1000, 10)  // TODO 10 should be configurable?
                .stream()
                .map(monitor -> {
                    try {
                        Optional<Date> nextRunTime = func.schedule(monitor);
                        if (nextRunTime.isPresent()) {
                            dao.updateNextSessionMonitorRunTime(monitor.getId(),
                                    nextRunTime.get().getTime() / 1000);
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

        public TaskStateCode getRootState(long sesId)
        {
            return TaskStateCode.of(dao.getRootState(siteId, sesId));
        }

        public List<StoredTask> getAllTasks()
        {
            return handle.createQuery(
                    selectTaskDetailsQuery() +
                    " where s.site_id = :siteId"
                )
                .bind("siteId", siteId)
                .map(stm)
                .list();
        }

        public List<StoredTask> getTasks(long sesId, int pageSize, Optional<Long> lastId)
        {
            return handle.createQuery(
                    selectTaskDetailsQuery() +
                    " where t.id > :lastId" +
                    " and s.site_id = :siteId" +
                    " and t.session_id = :sesId" +
                    " order by t.id" +
                    " limit :limit"
                )
                .bind("siteId", siteId)
                .bind("sesId", sesId)
                .bind("lastId", lastId.or(0L))
                .bind("limit", pageSize)
                .map(stm)
                .list();
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
                @Bind("namespaceId") int namespaceId, @Bind("name") String name, @Bind("params") Config params, @Bind("options") SessionOptions options);

        @SqlUpdate("insert into session_relations (id, repository_id, workflow_id)" +
                " values (:id, :repositoryId, :workflowId)")
        void insertSessionRelation(@Bind("id") long id,  @Bind("repositoryId") Integer repositoryId,
                @Bind("workflowId") Integer workflowId);

        @SqlQuery("select s.site_id, sr.repository_id, sr.workflow_id" +
                " from sessions s" +
                " join session_relations sr on sr.id = s.id" +
                " where s.id = :id")
        SessionRelation getSessionRelationById(@Bind("id") long sessionId);

        @SqlQuery("select state from tasks t" +
                " join sessions s on t.session_id = s.id" +
                " where s.site_id = :siteId" +
                " and s.id = :id" +
                " and t.parent_id is null" +
                " limit 1")
        short getRootState(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlUpdate("insert into session_monitors (session_id, config, next_run_time, created_at, updated_at)" +
                " values (:sessionId, :config, :nextRunTime, now(), now())")
        @GetGeneratedKeys
        long insertSessionMonitor(@Bind("sessionId") long sessionId, @Bind("config") Config config, @Bind("nextRunTime") long nextRunTime);

        @SqlQuery("select id from tasks where state = :state limit :limit")
        List<Long> findAllTaskIdsByState(@Bind("state") short state, @Bind("limit") int limit);

        @SqlUpdate("insert into tasks (session_id, parent_id, task_type, state, updated_at)" +
                " values (:sessionId, :parentId, :taskType, :state, now())")
        @GetGeneratedKeys
        long insertTask(@Bind("sessionId") long sessionId, @Bind("parentId") Long parentId,
                @Bind("taskType") int taskType, @Bind("state") short state);

        @SqlUpdate("insert into task_details (id, full_name, config)" +
                " values (:id, :fullName, :config)")
        void insertTaskDetails(@Bind("id") long id, @Bind("fullName") String fullName, @Bind("config") Config config);

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

        @SqlQuery("select id, parent_id, state, updated_at " +
                " from tasks" +
                " where session_id = :sessionId" +
                " and parent_id is null" +
                " for update")
        TaskStateSummary lockRootTask(@Bind("sessionId") long sessionId);

        @SqlQuery("select t.*, s.site_id, td.full_name, td.config, ts.state_params, ts.carry_params, ts.error, ts.report " +
                " from tasks t " +
                " join sessions s on s.id = t.session_id " +
                " join task_details td on t.id = td.id " +
                " join task_state_details ts on t.id = ts.id ")
        List<StoredTask> findAllTasks();

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
        long setStateDetails(@Bind("id") long taskId, @Bind("stateParams") Config stateParams, @Bind("carryParams") Config carryParams, @Bind("error") Config error, @Bind("report") Config report);

        @SqlUpdate("update tasks " +
                " set updated_at = now(), state = " + TaskStateCode.READY_CODE +
                " where state in (" + TaskStateCode.RETRY_WAITING_CODE +"," + TaskStateCode.GROUP_RETRY_WAITING_CODE + ")")
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

        @SqlUpdate("delete from session_monitors" +
                " where id = :id")
        void deleteSessionMonitor(@Bind("id") long id);
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
        private final ConfigMapper cfm;
        private final JsonMapper<SessionOptions> opm;

        public StoredSessionMapper(ConfigMapper cfm, JsonMapper<SessionOptions> opm)
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
        private final ConfigMapper cfm;
        private final JsonMapper<TaskReport> trm;

        public StoredTaskMapper(ConfigMapper cfm, JsonMapper<TaskReport> trm)
        {
            this.cfm = cfm;
            this.trm = trm;
        }

        @Override
        public StoredTask map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            String upstreamIdList = r.getString("upstream_ids");
            List<Long> upstreams;
            if (r.wasNull()) {
                upstreams = ImmutableList.of();
            }
            else {
                upstreams = Stream.of(upstreamIdList.split(","))
                    .map(it -> Long.parseLong(it))
                    .collect(Collectors.toList());
            }

            Config reportConfig = cfm.fromResultSetOrEmpty(r, "report");
            TaskReport report = TaskReport.builder()
                .carryParams(cfm.fromResultSetOrEmpty(r, "carry_params"))
                .inputs(reportConfig.getListOrEmpty("in", Config.class))
                .outputs(reportConfig.getListOrEmpty("out", Config.class))
                .build();

            return ImmutableStoredTask.builder()
                .id(r.getLong("id"))
                .siteId(r.getInt("site_id"))
                .upstreams(upstreams)
                .updatedAt(r.getTimestamp("updated_at"))
                .retryAt(getOptionalDate(r, "retry_at"))
                .stateParams(cfm.fromResultSetOrEmpty(r, "state_params"))
                .report(report)
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

    private static class SessionRelationMapper
            implements ResultSetMapper<SessionRelation>
    {
        @Override
        public SessionRelation map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableSessionRelation.builder()
                .siteId(r.getInt("site_id"))
                .repositoryId(getOptionalInt(r, "repository_id"))
                .workflowId(getOptionalInt(r, "workflow_id"))
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
                .sessionId(r.getInt("session_id"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .nextRunTime(new Date(r.getLong("next_run_time") * 1000))
                .createdAt(r.getTimestamp("created_at"))
                .updatedAt(r.getTimestamp("updated_at"))
                .build();
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
