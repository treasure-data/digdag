package io.digdag.core.database;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.schedule.ImmutableStoredSchedule;
import io.digdag.core.schedule.ScheduleControlStore;
import io.digdag.core.schedule.ScheduleStore;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.spi.ScheduleTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseScheduleStoreManager
        extends BasicDatabaseStoreManager<DatabaseScheduleStoreManager.Dao>
        implements ScheduleStoreManager
{
    @Inject
    public DatabaseScheduleStoreManager(TransactionManager transactionManager, ConfigMapper cfm, DatabaseConfig config)
    {
        super(config.getType(), Dao.class, transactionManager, cfm);
    }

    @Override
    public ScheduleStore getScheduleStore(int siteId)
    {
        return new DatabaseScheduleStore(siteId);
    }

    @Override
    public void lockReadySchedules(Instant currentTime, ScheduleAction func)
    {
        List<RuntimeException> exceptions = transaction((handle, dao) -> {
            return dao.lockReadySchedules(currentTime.getEpochSecond(), 10)  // TODO 10 should be configurable?
                .stream()
                .map(schedId -> {
                    // TODO JOIN + FOR UPDATE doesn't work with H2 database
                    StoredSchedule sched = dao.getScheduleByIdInternal(schedId);
                    try {
                        func.schedule(new DatabaseScheduleControlStore(handle), sched);
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

    private interface ScheduleCombinedLockAction <T, E extends Exception>
    {
        public T call(ScheduleControlStore store, StoredSchedule storedSched)
            throws ResourceNotFoundException, ResourceConflictException, E;
    }

    private class DatabaseScheduleStore
            implements ScheduleStore
    {
        // TODO retry
        private final int siteId;

        public DatabaseScheduleStore(int siteId)
        {
            this.siteId = siteId;
        }

        //public List<StoredSchedule> getAllSchedules()
        //{
        //    return dao.getSchedules(siteId, Integer.MAX_VALUE, 0L);
        //}

        @Override
        public List<StoredSchedule> getSchedules(int pageSize, Optional<Integer> lastId)
        {
            return autoCommit((handle, dao) -> dao.getSchedules(siteId, pageSize, lastId.or(0)));
        }

        @Override
        public StoredSchedule getScheduleById(int schedId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getScheduleById(siteId, schedId),
                    "schedule id=%d", schedId);
        }

        @Override
        public List<StoredSchedule> getSchedulesByProjectId(int projectId, int pageSize, Optional<Integer> lastId)
        {
            return autoCommit((handle, dao) -> dao.getSchedulesByProjectId(siteId, projectId, pageSize, lastId.or(0)));
        }

        @Override
        public StoredSchedule getScheduleByProjectIdAndWorkflowName(int projectId, String workflowName)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getScheduleByProjectIdAndWorkflowName(siteId, projectId, workflowName),
                    "schedule projectId=%d workflowName", projectId, workflowName);
        }

        private <T, E extends Exception> T combinedLockScheduleById(int schedId, ScheduleCombinedLockAction<T, E> func, Class<E> exClass)
            throws ResourceNotFoundException, ResourceConflictException, E
        {
            return DatabaseScheduleStoreManager.this.<T, ResourceNotFoundException, ResourceConflictException, E>transaction((handle, dao) -> {
                // JOIN + FOR UPDATE doesn't work with H2 database. So here locks it first then get columns.
                if (dao.lockScheduleById(schedId) == 0) {
                    throw new ResourceNotFoundException("schedule id="+schedId);
                }
                StoredSchedule schedule = requiredResource(
                        dao.getScheduleByIdInternal(schedId),
                        "schedule id=%d", schedId);
                return func.call(new DatabaseScheduleControlStore(handle), schedule);
            }, ResourceNotFoundException.class, ResourceConflictException.class, exClass);
        }

        @Override
        public <T> T updateScheduleById(int schedId, ScheduleUpdateAction<T> func)
            throws ResourceNotFoundException, ResourceConflictException
        {
            return combinedLockScheduleById(schedId, (store, sched) -> func.call(store, sched), RuntimeException.class);
        }

        @Override
        public <T> T lockScheduleById(int schedId, ScheduleLockAction<T> func)
            throws ResourceNotFoundException, ResourceConflictException, ResourceLimitExceededException
        {
            return combinedLockScheduleById(schedId, (store, sched) -> func.call(store, sched), ResourceLimitExceededException.class);
        }
    }

    private static class DatabaseScheduleControlStore
            implements ScheduleControlStore
    {
        private final Handle handle;
        private final Dao dao;

        public DatabaseScheduleControlStore(Handle handle)
        {
            this.handle = handle;
            this.dao = handle.attach(Dao.class);
        }

        @Override
        public void updateNextScheduleTime(int schedId, ScheduleTime nextTime)
            throws ResourceNotFoundException
        {
            int n = dao.updateNextScheduleTime(schedId,
                    nextTime.getRunTime().getEpochSecond(),
                    nextTime.getTime().getEpochSecond());
            assert n >= 0;
            if (n <= 0) {
                throw new ResourceNotFoundException("schedule id=" + schedId);
            }
        }

        @Override
        public void updateNextScheduleTimeAndLastSessionTime(int schedId, ScheduleTime nextTime,
                Instant lastSessionTime)
            throws ResourceNotFoundException
        {
            int n = dao.updateNextScheduleTime(schedId,
                    nextTime.getRunTime().getEpochSecond(),
                    nextTime.getTime().getEpochSecond(),
                    lastSessionTime.getEpochSecond());
            assert n >= 0;
            if (n <= 0) {
                throw new ResourceNotFoundException("schedule id=" + schedId);
            }
        }

        @Override
        public boolean disableSchedule(int schedId)
        {
            int n = dao.disableSchedule(schedId);
            return n > 0;
        }

        @Override
        public boolean enableSchedule(int schedId)
        {
            int n = dao.enableSchedule(schedId);
            return n > 0;
        }

        @Override
        public StoredSchedule getScheduleById(int schedId)
        {
            return dao.getScheduleByIdInternal(schedId);
        }
    }

    public interface Dao
    {
        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.id = :schedId")
        StoredSchedule getScheduleByIdInternal(@Bind("schedId") int schedId);

        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where exists (" +
                    "select * from projects proj" +
                    " where proj.id = s.project_id" +
                    " and proj.site_id = :siteId" +
                ")" +
                " and s.id > :lastId" +
                " order by s.id asc" +
                " limit :limit")
        List<StoredSchedule> getSchedules(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.project_id = :projectId " +
                " and exists (" +
                    "select * from projects proj" +
                    " where proj.id = s.project_id" +
                    " and proj.site_id = :siteId" +
                ")" +
                " and s.id > :lastId" +
                " order by s.id asc" +
                " limit :limit")
        List<StoredSchedule> getSchedulesByProjectId(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.id = :schedId" +
                " and exists (" +
                    "select * from projects proj" +
                    " where proj.id = s.project_id" +
                    " and proj.site_id = :siteId" +
                ")")
        StoredSchedule getScheduleById(@Bind("siteId") int siteId, @Bind("schedId") int schedId);

        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where wd.name = :workflowName" +
                " and s.project_id = :projectId" +
                " and exists (" +
                    "select * from projects proj" +
                    " where proj.id = s.project_id" +
                    " and proj.site_id = :siteId" +
                ")")
        StoredSchedule getScheduleByProjectIdAndWorkflowName(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("workflowName") String workflowName);

        @SqlQuery("select id from schedules" +
                " where next_run_time <= :currentTime" +
                " and disabled_at is null" +
                " limit :limit" +
                " for update")
        List<Integer> lockReadySchedules(@Bind("currentTime") long currentTime, @Bind("limit") int limit);

        @SqlQuery("select * from schedules" +
                " where id = :id" +
                " for update")
        int lockScheduleById(@Bind("id") long schedId);

        @SqlUpdate("update schedules" +
                " set next_run_time = :nextRunTime, next_schedule_time = :nextScheduleTime, updated_at = now()" +
                " where id = :id")
        int updateNextScheduleTime(@Bind("id") int id, @Bind("nextRunTime") long nextRunTime, @Bind("nextScheduleTime") long nextScheduleTime);

        @SqlUpdate("update schedules" +
                " set next_run_time = :nextRunTime, next_schedule_time = :nextScheduleTime, last_session_time = :lastSessionTime, updated_at = now()" +
                " where id = :id")
        int updateNextScheduleTime(@Bind("id") int id, @Bind("nextRunTime") long nextRunTime, @Bind("nextScheduleTime") long nextScheduleTime, @Bind("lastSessionTime") long lastSessionTime);

        @SqlUpdate("update schedules" +
                " set disabled_at = now(), updated_at = now()" +
                " where id = :id")
        int disableSchedule(@Bind("id") int id);

        @SqlUpdate("update schedules" +
                " set disabled_at = null, updated_at = now()" +
                " where id = :id")
        int enableSchedule(@Bind("id") int id);
    }

    // TODO
    public static class StoredScheduleMapper
            implements ResultSetMapper<StoredSchedule>
    {
        private final ConfigMapper cfm;

        public StoredScheduleMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredSchedule map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredSchedule.builder()
                .id(r.getInt("id"))
                .projectId(r.getInt("project_id"))
                .workflowDefinitionId(r.getLong("workflow_definition_id"))
                .nextRunTime(Instant.ofEpochSecond(r.getLong("next_run_time")))
                .nextScheduleTime(Instant.ofEpochSecond(r.getLong("next_schedule_time")))
                .workflowName(r.getString("name"))
                .createdAt(getTimestampInstant(r, "created_at"))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .disabledAt(getOptionalTimestampInstant(r, "disabled_at"))
                .lastSessionTime(getOptionalLong(r, "last_session_time").transform(Instant::ofEpochSecond))
                .build();
        }
    }
}
