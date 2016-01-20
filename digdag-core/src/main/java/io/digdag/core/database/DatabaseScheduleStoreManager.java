package io.digdag.core.database;

import java.util.List;
import java.util.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import com.google.common.primitives.Longs;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.schedule.Schedule;
import io.digdag.core.schedule.ScheduleStore;
import io.digdag.core.schedule.ScheduleStoreManager;
import io.digdag.core.schedule.ScheduleControl;
import io.digdag.core.schedule.ScheduleControlStore;
import io.digdag.core.schedule.StoredSchedule;
import io.digdag.core.schedule.ImmutableStoredSchedule;
import io.digdag.spi.ScheduleTime;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import io.digdag.spi.config.Config;

public class DatabaseScheduleStoreManager
        extends BasicDatabaseStoreManager
        implements ScheduleStoreManager, ScheduleControlStore
{
    private final StoredScheduleMapper ssm;
    private final ConfigMapper cfm;
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseScheduleStoreManager(IDBI dbi, ConfigMapper cfm)
    {
        this.handle = dbi.open();
        this.cfm = cfm;
        this.ssm = new StoredScheduleMapper(cfm);
        handle.registerMapper(ssm);
        handle.registerArgumentFactory(cfm.getArgumentFactory());
        this.dao = handle.attach(Dao.class);
    }

    public void close()
    {
        handle.close();
    }

    @Override
    public ScheduleStore getScheduleStore(int siteId)
    {
        return new DatabaseScheduleStore(siteId);
    }

    @Override
    public void lockReadySchedules(Date currentTime, ScheduleAction func)
    {
        List<RuntimeException> exceptions = handle.inTransaction((handle, session) -> {
            return dao.lockReadySchedules(currentTime.getTime() / 1000, 10)  // TODO 10 should be configurable?
                .stream()
                .map(schedId -> {
                    // TODO JOIN + FOR UPDATE doesn't work with H2 database
                    StoredSchedule sched = dao.getScheduleByIdInternal(schedId);
                    try {
                        ScheduleTime nextTime = func.schedule(sched);
                        dao.updateNextScheduleTime(sched.getId(),
                                nextTime.getRunTime().getTime() / 1000,
                                nextTime.getScheduleTime().getTime() / 1000);
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
    public boolean updateNextScheduleTime(long schedId,
            ScheduleTime nextTime)
    {
        int n = dao.updateNextScheduleTime(schedId,
                nextTime.getRunTime().getTime() / 1000,
                nextTime.getScheduleTime().getTime() / 1000);
        return n > 0;
    }

    public <T> T lockScheduleById(long schedId, ScheduleLockAction<T> func)
        throws ResourceNotFoundException
    {
        Optional<T> ret = handle.inTransaction((handle, session) -> {
            // TODO JOIN + FOR UPDATE doesn't work with H2 database
            dao.lockScheduleById(schedId);
            StoredSchedule schedule = dao.getScheduleByIdInternal(schedId);
            if (schedule == null) {
                return Optional.<T>absent();
            }
            ScheduleControl control = new ScheduleControl(this, schedule);
            T result = func.call(control);
            return Optional.of(result);
        });
        return requiredResource(
                ret.orNull(),
                "schedule id=%d", schedId);
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
        public List<StoredSchedule> getSchedules(int pageSize, Optional<Long> lastId)
        {
            return dao.getSchedules(siteId, pageSize, lastId.or(0L));
        }

        @Override
        public StoredSchedule getScheduleById(long schedId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getScheduleById(siteId, schedId),
                    "schedule id=%d", schedId);
        }
    }

    public interface Dao
    {
        @SqlQuery("select s.*, ss.name as name, ss.config as config from schedules s" +
                " join schedule_sources ss on s.source_id = ss.id" +
                " join revisions rev on ss.revision_id = rev.id" +
                " join repositories repo on rev.repository_id = repo.id" +
                " where repo.site_id = :siteId" +
                " and s.id > :lastId" +
                " order by s.id asc" +
                " limit :limit")
        List<StoredSchedule> getSchedules(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select s.*, ss.name as name, ss.config as config from schedules s" +
                " join schedule_sources ss on s.source_id = ss.id" +
                " where s.id = :schedId")
        StoredSchedule getScheduleByIdInternal(@Bind("schedId") long schedId);

        @SqlQuery("select s.*, ss.name as name, ss.config as config from schedules s" +
                " join schedule_sources ss on s.source_id = ss.id" +
                " join revisions rev on ss.revision_id = rev.id" +
                " join repositories repo on rev.repository_id = repo.id" +
                " where repo.site_id = :siteId" +
                " and s.id = :schedId")
        StoredSchedule getScheduleById(@Bind("siteId") int siteId, @Bind("schedId") long schedId);

        @SqlUpdate("insert into schedules" +
                " (source_id, workflow_id, next_run_time, next_schedule_time, created_at, updated_at)" +
                " values (:sourceId, :workflowId, :nextRunTime, :nextScheduleTime, now(), now())")
        @GetGeneratedKeys
        long insertRepositorySchedule(@Bind("sourceId") int sourceId, @Bind("workflowId") int workflowId, @Bind("nextRunTime") long nextRunTime, @Bind("nextScheduleTime") long nextScheduleTime);

        @SqlQuery("select id from schedules" +
                " where next_run_time <= :currentTime" +
                " limit :limit" +
                " for update")
        List<Integer> lockReadySchedules(@Bind("currentTime") long currentTime, @Bind("limit") int limit);

        @SqlQuery("select * from schedules" +
                " where id = :id" +
                " for update")
        int lockScheduleById(@Bind("id") long taskId);

        @SqlUpdate("update schedules" +
                " set next_run_time = :nextRunTime, next_schedule_time = :nextScheduleTime, updated_at = now()" +
                " where id = :id")
        int updateNextScheduleTime(@Bind("id") long id, @Bind("nextRunTime") long nextRunTime, @Bind("nextScheduleTime") long nextScheduleTime);
    }

    private static class StoredScheduleMapper
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
                .id(r.getLong("id"))
                .scheduleSourceId(r.getInt("source_id"))
                .workflowSourceId(r.getInt("workflow_id"))
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .nextRunTime(new Date(r.getLong("next_run_time") * 1000))
                .nextScheduleTime(new Date(r.getLong("next_schedule_time") * 1000))
                .createdAt(r.getTimestamp("created_at"))
                .updatedAt(r.getTimestamp("updated_at"))
                .build();
        }
    }
}
