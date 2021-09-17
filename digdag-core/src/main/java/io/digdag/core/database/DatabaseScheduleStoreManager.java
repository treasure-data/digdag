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
import io.digdag.spi.AccountRouting;
import io.digdag.spi.ScheduleTime;
import io.digdag.spi.ac.AccessController;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Define;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class DatabaseScheduleStoreManager
        extends BasicDatabaseStoreManager<DatabaseScheduleStoreManager.Dao>
        implements ScheduleStoreManager
{
    @Inject
    public DatabaseScheduleStoreManager(TransactionManager transactionManager, ConfigMapper cfm, DatabaseConfig config)
    {
        super(config.getType(), dao(config.getType()), transactionManager, cfm);
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

    @Override
    public ScheduleStore getScheduleStore(int siteId)
    {
        return new DatabaseScheduleStore(siteId);
    }

    @Override
    public int lockReadySchedules(Instant currentTime, int limit, AccountRouting accountRouting, ScheduleAction func)
    {
        List<RuntimeException> exceptions = new ArrayList<>();

        long count = transaction((handle, dao) -> {
            Stream<StoredSchedule> schedStream;
            if (dao instanceof PgDao) {
                if (accountRouting.enabled()) {
                    schedStream = ((PgDao) dao).lockReadySchedulesSkipLockedWithAccountFilter(currentTime.getEpochSecond(), limit, accountRouting.getFilterSQL()).stream();
                }
                else {
                    schedStream = ((PgDao) dao).lockReadySchedulesSkipLocked(currentTime.getEpochSecond(), limit).stream();
                }
            }
            else {
                // H2 database doesn't support JOIN + FOR UPDATE OF
                List<Integer> list1;
                if (accountRouting.enabled()) {
                    list1 = dao.lockReadyScheduleIdsWithAccountFilter(currentTime.getEpochSecond(), limit, accountRouting.getFilterSQL());
                }
                else {
                    list1 = dao.lockReadyScheduleIds(currentTime.getEpochSecond(), limit);
                }
                schedStream = list1.stream().map(dao::getScheduleByIdInternal);
            }
            return schedStream.mapToInt(sched -> {
                    try {
                        func.schedule(new DatabaseScheduleControlStore(handle), sched);
                    }
                    catch (RuntimeException ex) {
                        exceptions.add(ex);
                    }
                    return 1;
                })
                .sum();
        });

        if (!exceptions.isEmpty()) {
            RuntimeException first = exceptions.get(0);
            for (RuntimeException ex : exceptions.subList(1, exceptions.size())) {
                first.addSuppressed(ex);
            }
            throw first;
        }

        return (int) count;
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
        public List<StoredSchedule> getSchedules(int pageSize, Optional<Integer> lastId, AccessController.ListFilter acFilter)
        {
            return autoCommit((handle, dao) -> dao.getSchedules(siteId, pageSize, lastId.or(0), acFilter.getSql()));
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
        public List<StoredSchedule> getSchedulesByProjectId(int projectId, int pageSize, Optional<Integer> lastId, AccessController.ListFilter acFilter)
        {
            return autoCommit((handle, dao) -> dao.getSchedulesByProjectId(siteId, projectId, pageSize, lastId.or(0), acFilter.getSql()));
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

    @UseStringTemplate3StatementLocator
    public interface H2Dao
            extends Dao
    {
    }

    @UseStringTemplate3StatementLocator
    public interface PgDao
            extends Dao
    {
        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.next_run_time \\<= :currentTime" +
                " and s.disabled_at is null" +
                " limit :limit" +
                " for update of s skip locked")
        List<StoredSchedule> lockReadySchedulesSkipLocked(@Bind("currentTime") long currentTime, @Bind("limit") int limit);

        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.next_run_time \\<= :currentTime" +
                " and s.disabled_at is null" +
                " and exists ( select site_id from projects p where s.project_id = p.id" +
                "   and <accountFilter> )" +
                " limit :limit" +
                " for update of s skip locked")
        List<StoredSchedule> lockReadySchedulesSkipLockedWithAccountFilter(@Bind("currentTime") long currentTime, @Bind("limit") int limit, @Define("accountFilter") String accountFilter);
    }

    public interface Dao
    {
        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.id = :schedId")
        StoredSchedule getScheduleByIdInternal(@Bind("schedId") int schedId);

        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " join projects proj on proj.id = s.project_id" +
                " where exists (" +
                    "select p.* from projects p" +
                    " where p.id = s.project_id" +
                    " and p.site_id = :siteId" +
                ")" +
                " and s.id > :lastId" +
                " and <acFilter>" +
                " order by s.id asc" +
                " limit :limit")
        List<StoredSchedule> getSchedules(
                @Bind("siteId") int siteId,
                @Bind("limit") int limit,
                @Bind("lastId") int lastId,
                @Define("acFilter") String acFilter);

        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " join projects proj on proj.id = s.project_id" +
                " where s.project_id = :projectId " +
                " and exists (" +
                    "select p.* from projects p" +
                    " where p.id = s.project_id" +
                    " and p.site_id = :siteId" +
                ")" +
                " and s.id > :lastId" +
                " and <acFilter>" +
                " order by s.id asc" +
                " limit :limit")
        List<StoredSchedule> getSchedulesByProjectId(
                @Bind("siteId") int siteId,
                @Bind("projectId") int projectId,
                @Bind("limit") int limit,
                @Bind("lastId") int lastId,
                @Define("acFilter") String acFilter);

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
                " where next_run_time \\<= :currentTime" +
                " and disabled_at is null" +
                " limit :limit" +
                " for update")
        List<Integer> lockReadyScheduleIds(@Bind("currentTime") long currentTime, @Bind("limit") int limit);

        @SqlQuery("select id from schedules" +
                " where next_run_time \\<= :currentTime" +
                " and disabled_at is null" +
                " and exists ( select site_id from projects p where schedules.project_id = p.id" +
                "   and <accountFilter> )" +
                " limit :limit" +
                " for update")
        List<Integer> lockReadyScheduleIdsWithAccountFilter(@Bind("currentTime") long currentTime, @Bind("limit") int limit, @Define("accountFilter") String accountFilter);

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

    static class StoredScheduleMapper
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
