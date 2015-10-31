package io.digdag.core;

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
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

public class DatabaseScheduleStoreManager
        extends BasicDatabaseStoreManager
        implements ScheduleStoreManager
{
    private final StoredScheduleMapper ssm;
    private final ConfigSourceMapper cfm;
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseScheduleStoreManager(IDBI dbi, ConfigSourceMapper cfm)
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

    public ScheduleStore getScheduleStore(int siteId)
    {
        return new DatabaseScheduleStore(siteId);
    }

    public void lockReadySchedules(Date currentTime, ScheduleAction func)
    {
        List<RuntimeException> exceptions = handle.inTransaction((handle, session) -> {
            return dao.lockReadySchedules(currentTime.getTime() / 1000, 10)  // TODO 10 should be configurable?
                .stream()
                .map(sched -> {
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

    private class DatabaseScheduleStore
            implements ScheduleStore
    {
        // TODO retry
        private final int siteId;

        public DatabaseScheduleStore(int siteId)
        {
            this.siteId = siteId;
        }

        public <T> T transaction(StoreTransaction<T> transaction)
        {
            return handle.inTransaction((handle, session) -> transaction.call());
        }

        public List<StoredSchedule> getAllSchedules()
        {
            return dao.getSchedules(siteId, Integer.MAX_VALUE, 0L);
        }

        public List<StoredSchedule> getSchedules(int pageSize, Optional<Long> lastId)
        {
            return dao.getSchedules(siteId, pageSize, lastId.or(0L));
        }

        public List<StoredSchedule> syncRepositorySchedules(int repoId, List<Schedule> schedules)
        {
            return handle.inTransaction((handle, session) -> {
                final long[] ids = new long[schedules.size()];
                int i = 0;
                for (Schedule schedule : schedules) {
                    ids[i] = dao.insertRepositorySchedule(
                            schedule.getWorkflowId(), schedule.getScheduleType().get(), schedule.getConfig(),
                            schedule.getNextRunTime().getTime() / 1000,
                            schedule.getNextScheduleTime().getTime() / 1000);
                    i++;
                }
                String idList = Longs.asList(ids).stream().map(id -> Long.toString(id)).collect(Collectors.joining(","));
                handle.createStatement(
                        "delete from schedules" +
                        " where id in ("+
                            "select s.id from schedules s" +
                            " join workflows wf on s.workflow_id = wf.id" +
                            " join revisions rev on wf.revision_id = rev.id" +
                            " where rev.repository_id = :repoId" +
                            " and s.id not in (" + idList + ")" +
                        ")"
                    )
                    .bind("repoId", repoId)
                    .execute();
                return handle.createQuery(
                        "select * from schedules" +
                        " where id in (" + idList + ")"
                    )
                    .map(ssm)
                    .list();
            });
        }
    }

    public interface Dao
    {
        @SqlQuery("select * from schedules s" +
                " join workflows wf on s.workflow_id = wf.id" +
                " join revisions rev on wf.revision_id = rev.id" +
                " join repositories repo on rev.repository_id = repo.id" +
                " where repo.site_id = :siteId" +
                " and id > :lastId" +
                " order by id desc" +
                " limit :limit")
        List<StoredSchedule> getSchedules(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlUpdate("insert into schedules" +
                " (workflow_id, schedule_type, config, next_run_time, next_schedule_time, created_at, updated_at)" +
                " values (:workflowId, :scheduleType, :config, :nextRunTime, :nextScheduleTime, now(), now())")
        @GetGeneratedKeys
        long insertRepositorySchedule(@Bind("workflowId") int workflowId, @Bind("scheduleType") int scheduleType, @Bind("config") ConfigSource config, @Bind("nextRunTime") long nextRunTime, @Bind("nextScheduleTime") long nextScheduleTime);

        @SqlQuery("select * from schedules" +
                " where next_run_time <= :currentTime" +
                " limit :limit" +
                " for update")
        List<StoredSchedule> lockReadySchedules(@Bind("currentTime") long currentTime, @Bind("limit") int limit);

        @SqlUpdate("update schedules" +
                " set next_run_time = :nextRunTime, next_schedule_time = :nextScheduleTime, updated_at = now()" +
                " where id = :id")
        void updateNextScheduleTime(@Bind("id") long id, @Bind("nextRunTime") long nextRunTime, @Bind("nextScheduleTime") long nextScheduleTime);
    }

    private static class StoredScheduleMapper
            implements ResultSetMapper<StoredSchedule>
    {
        private final ConfigSourceMapper cfm;

        public StoredScheduleMapper(ConfigSourceMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredSchedule map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredSchedule.builder()
                .id(r.getLong("id"))
                .workflowId(r.getInt("workflow_id"))
                .scheduleType(ScheduleType.of(r.getInt("schedule_type")))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .nextRunTime(new Date(r.getLong("next_run_time") * 1000))
                .nextScheduleTime(new Date(r.getLong("next_schedule_time") * 1000))
                .createdAt(r.getTimestamp("created_at"))
                .updatedAt(r.getTimestamp("updated_at"))
                .build();
        }
    }
}
