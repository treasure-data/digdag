package io.digdag.core.database;

import java.util.List;
import java.time.Instant;
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
import io.digdag.client.config.Config;

public class DatabaseScheduleStoreManager
        extends BasicDatabaseStoreManager
        implements ScheduleStoreManager, ScheduleControlStore
{
    private final ConfigMapper cfm;
    private final Dao dao;

    @Inject
    public DatabaseScheduleStoreManager(IDBI dbi, ConfigMapper cfm, DatabaseStoreConfig config)
    {
        super(config.getType(), dbi.open());
        this.cfm = cfm;
        handle.registerMapper(new StoredScheduleMapper(cfm));
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
    public void lockReadySchedules(Instant currentTime, ScheduleAction func)
    {
        List<RuntimeException> exceptions = transaction(ts -> {
            return dao.lockReadySchedules(currentTime.getEpochSecond(), 10)  // TODO 10 should be configurable?
                .stream()
                .map(schedId -> {
                    // TODO JOIN + FOR UPDATE doesn't work with H2 database
                    StoredSchedule sched = dao.getScheduleByIdInternal(schedId);
                    try {
                        func.schedule(this, sched);
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
    public boolean updateNextScheduleTime(long schedId, ScheduleTime nextTime)
    {
        int n = dao.updateNextScheduleTime(schedId,
                nextTime.getRunTime().getEpochSecond(),
                nextTime.getScheduleTime().getEpochSecond());
        return n > 0;
    }

    @Override
    public boolean updateNextScheduleTime(long schedId, ScheduleTime nextTime,
            Instant lastSessionInstant)
    {
        int n = dao.updateNextScheduleTime(schedId,
                nextTime.getRunTime().getEpochSecond(),
                nextTime.getScheduleTime().getEpochSecond(),
                lastSessionInstant.getEpochSecond());
        return n > 0;
    }

    public <T> T lockScheduleById(long schedId, ScheduleLockAction<T> func)
        throws ResourceNotFoundException
    {
        Optional<T> ret = transaction(ts -> {
            // TODO JOIN + FOR UPDATE doesn't work with H2 database
            dao.lockScheduleById(schedId);
            StoredSchedule schedule = dao.getScheduleByIdInternal(schedId);
            if (schedule == null) {
                return Optional.<T>absent();
            }
            T result = func.call(this, schedule);
            return Optional.of(result);
        }, ResourceNotFoundException.class);
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
        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.id = :schedId")
        StoredSchedule getScheduleByIdInternal(@Bind("schedId") long schedId);

        @SqlQuery("select s.*, wd.name as name from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.repository_id in (" +
                    "select id from repositories repo" +
                    " where repo.site_id = :siteId" +
                ")" +
                " and s.id > :lastId" +
                " order by s.id asc" +
                " limit :limit")
        List<StoredSchedule> getSchedules(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlQuery("select s.*, wd.name as name, from schedules s" +
                " join workflow_definitions wd on wd.id = s.workflow_definition_id" +
                " where s.id = :schedId" +
                " and exists (" +
                    "select * from repositories repo" +
                    " where repo.id = s.repository_id" +
                    " and repo.site_id = :siteId" +
                ")")
        StoredSchedule getScheduleById(@Bind("siteId") int siteId, @Bind("schedId") long schedId);

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

        @SqlUpdate("update schedules" +
                " set next_run_time = :nextRunTime, next_schedule_time = :nextScheduleTime, last_session_instant = :lastSessionInstant, updated_at = now()" +
                " where id = :id")
        int updateNextScheduleTime(@Bind("id") long id, @Bind("nextRunTime") long nextRunTime, @Bind("nextScheduleTime") long nextScheduleTime, @Bind("lastSessionInstant") long lastSessionInstant);
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
                .id(r.getInt("id"))
                .repositoryId(r.getInt("repository_id"))
                .workflowDefinitionId(r.getLong("workflow_definition_id"))
                .nextRunTime(Instant.ofEpochSecond(r.getLong("next_run_time")))
                .nextScheduleTime(Instant.ofEpochSecond(r.getLong("next_schedule_time")))
                .workflowName(r.getString("name"))
                .createdAt(getTimestampInstant(r, "created_at"))
                .updatedAt(getTimestampInstant(r, "updated_at"))
                .build();
        }
    }
}
