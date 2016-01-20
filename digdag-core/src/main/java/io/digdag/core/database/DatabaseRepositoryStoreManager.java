package io.digdag.core.database;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.Schedule;
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
import static com.google.common.base.Preconditions.checkArgument;

public class DatabaseRepositoryStoreManager
        extends BasicDatabaseStoreManager
        implements RepositoryStoreManager
{
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseRepositoryStoreManager(IDBI dbi, ConfigMapper cfm)
    {
        this.handle = dbi.open();
        handle.registerMapper(new StoredRepositoryMapper(cfm));
        handle.registerMapper(new StoredRevisionMapper(cfm));
        handle.registerMapper(new StoredWorkflowSourceMapper(cfm));
        handle.registerMapper(new StoredScheduleSourceMapper(cfm));
        handle.registerMapper(new StoredWorkflowSourceWithRepositoryMapper(cfm));
        handle.registerArgumentFactory(cfm.getArgumentFactory());
        this.dao = handle.attach(Dao.class);
    }

    @Override
    public RepositoryStore getRepositoryStore(int siteId)
    {
        return new DatabaseRepositoryStore(siteId);
    }

    @Override
    public StoredWorkflowSourceWithRepository getWorkflowDetailsById(int wfId)
            throws ResourceNotFoundException
    {
        return requiredResource(
                dao.getWorkflowDetailsById(wfId),
                "workflow id=%s", wfId);
    }

    private class DatabaseRepositoryStore
            implements RepositoryStore, RepositoryControlStore
    {
        // TODO retry
        private final int siteId;

        public DatabaseRepositoryStore(int siteId)
        {
            this.siteId = siteId;
        }

        //public List<StoredRepository> getAllRepositories()
        //{
        //    return dao.getRepositories(siteId, Integer.MAX_VALUE, 0);
        //}

        @Override
        public List<StoredRepository> getRepositories(int pageSize, Optional<Integer> lastId)
        {
            return dao.getRepositories(siteId, pageSize, lastId.or(0));
        }

        @Override
        public StoredRepository getRepositoryById(int repoId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getRepositoryById(siteId, repoId),
                    "repository id=%d", repoId);
        }

        @Override
        public StoredRepository getRepositoryByName(String repoName)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getRepositoryByName(siteId, repoName),
                    "repository name=%s", repoName);
        }

        @Override
        public <T> T putRepository(Repository repository, RepositoryLockAction<T> func)
        {
            return handle.inTransaction((handle, session) -> {
                StoredRepository repo;
                try {
                    int repoId = catchConflict(() ->
                            dao.insertRepository(siteId, repository.getName()),
                            "repository name=%s", repository.getName());
                    repo = getRepositoryById(repoId);
                    if (repo == null) {
                        throw new IllegalStateException("Database state error");
                    }
                }
                catch (ResourceConflictException ex) {
                    repo = dao.getRepositoryByName(siteId, repository.getName());
                    if (repo == null) {
                        throw new IllegalStateException("Database state error", ex);
                    }
                }
                RepositoryControl control = new RepositoryControl(this, repo);
                return func.call(control);
            });
        }

        //public List<StoredRevision> getAllRevisions(int repoId)
        //{
        //    return dao.getRevisions(siteId, repoId, Integer.MAX_VALUE, 0);
        //}

        @Override
        public List<StoredRevision> getRevisions(int repoId, int pageSize, Optional<Integer> lastId)
        {
            return dao.getRevisions(siteId, repoId, pageSize, lastId.or(0));
        }

        @Override
        public StoredRevision getRevisionById(int revId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getRevisionById(siteId, revId),
                    "revision id=%d", revId);
        }

        @Override
        public StoredRevision getRevisionByName(int repoId, String revName)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getRevisionByName(siteId, repoId, revName),
                    "revision name=%s in repository id=%d", revName, repoId);
        }

        @Override
        public StoredRevision getLatestActiveRevision(int repoId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getLatestActiveRevision(siteId, repoId),
                    "repository id=%d", repoId);
        }

        /**
         * Create or overwrite a revision.
         *
         * This method doesn't check site id because RepositoryControl
         * interface is avaiable only if site is is valid.
         */
        @Override
        public StoredRevision putRevision(int repoId, Revision revision)
        {
            try {
                try {
                    int revId = catchConflict(() ->
                        dao.insertRevision(repoId, revision.getName(), revision.getDefaultParams(), revision.getArchiveType(), revision.getArchiveMd5().orNull(), revision.getArchivePath().orNull(), revision.getArchiveData().orNull()),
                        "revision=%s in repository id=%d", revision.getName(), repoId);
                    return getRevisionById(revId);
                }
                catch (ResourceConflictException ex) {
                    StoredRevision rev = getRevisionByName(repoId, revision.getName());
                    if (revision.equals(Revision.revisionBuilder().from(rev).build())) {
                        return rev;
                    }
                    // TODO once implement deleteRevision, delete the current revision and overwrite it
                    throw new UnsupportedOperationException("Revision already exists. Overwriting an existing revision is not supported.", ex);
                }
            }
            catch (ResourceNotFoundException ex) {
                throw new IllegalStateException("Database state error", ex);
            }
        }


        //public List<StoredWorkflowSource> getAllWorkflowSources(int revId)
        //{
        //    return dao.getWorkflowSources(siteId, revId, Integer.MAX_VALUE, 0);
        //}

        @Override
        public List<StoredWorkflowSource> getWorkflowSources(int revId, int pageSize, Optional<Integer> lastId)
        {
            return dao.getWorkflowSources(siteId, revId, pageSize, lastId.or(0));
        }

        @Override
        public List<StoredWorkflowSourceWithRepository> getLatestActiveWorkflowSources(int pageSize, Optional<Integer> lastId)
        {
            return dao.getLatestActiveWorkflowSources(siteId, pageSize, lastId.or(0));
        }

        @Override
        public StoredWorkflowSource getWorkflowSourceById(int wfId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getWorkflowSourceByid(siteId, wfId),
                    "workflow id=%d", wfId);
        }

        @Override
        public StoredWorkflowSource getWorkflowSourceByName(int revId, String name)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getWorkflowSourceByName(siteId, revId, name),
                    "workflow name=%s in revision id=%d", name, revId);
        }

        //@Override
        public StoredScheduleSource getScheduleSourceById(int wfId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getScheduleSourceByid(siteId, wfId),
                    "schedule id=%d", wfId);
        }

        //@Override
        public StoredScheduleSource getScheduleSourceByName(int revId, String name)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getScheduleSourceByName(siteId, revId, name),
                    "schedule name=%s in revision id=%d", name, revId);
        }

        /**
         * Create a revision.
         *
         * This method doesn't check site id because RepositoryControl
         * interface is avaiable only if site is is valid.
         */
        @Override
        public StoredWorkflowSource insertWorkflowSource(int revId, WorkflowSource source)
            throws ResourceConflictException
        {
            try {
                int wfId = catchConflict(() ->
                    dao.insertWorkflowSource(revId, source.getName(), source.getConfig()),
                    "workflow=%s in revision id=%d", source.getName(), revId);
                return getWorkflowSourceById(wfId);
            }
            catch (ResourceNotFoundException ex) {
                throw new IllegalStateException("Database state error", ex);
            }
        }

        @Override
        public StoredScheduleSource insertScheduleSource(int revId, ScheduleSource source)
            throws ResourceConflictException
        {
            try {
                int wfId = catchConflict(() ->
                    dao.insertScheduleSource(revId, source.getName(), source.getConfig()),
                    "schedule=%s in revision id=%d", source.getName(), revId);
                return getScheduleSourceById(wfId);
            }
            catch (ResourceNotFoundException ex) {
                throw new IllegalStateException("Database state error", ex);
            }
        }

        @Override
        public void syncWorkflowsToRevision(int repoId, List<StoredWorkflowSource> sources)
            throws ResourceConflictException
        {
            if (sources.isEmpty()) {
                return;
            }
            int revId = sources.get(0).getRevisionId();

            Map<String, Integer> oldNames = dao.getLatestWorkflowNamesOfRepository(repoId);
            if (oldNames == null) {
                oldNames = ImmutableMap.of();
            }

            for (StoredWorkflowSource source : sources) {
                // validate workflows
                checkArgument(source.getRevisionId() == revId);

                if (oldNames.containsKey(source.getName())) {
                    // found the same name. overwriting source_id.
                    int n = handle.createStatement(
                            "update workflows" +
                            " set source_id = :sourceId" +
                            " where id = :id"
                        )
                        .bind("sourceId", source.getId())
                        .bind("id", oldNames.get(source.getName()))
                        .execute();
                    if (n <= 0) {
                        // TODO exception?
                    }
                    oldNames.remove(source.getName());
                }
                else {
                    // not found this name. inserting new entry.
                    // TODO this INSERT can be optimized by using lazy multiple-value insert
                    catchConflict(() ->
                            handle.createStatement(
                                "insert into workflows" +
                                " (source_id, repository_id, name)" +
                                " values (:sourceId, :repoId, :name)"
                            )
                            .bind("sourceId", source.getId())
                            .bind("repoId", repoId)
                            .bind("name", source.getName())
                            .execute(),
                            "workflow name=%s", source.getName());
                }
            }
            if (!oldNames.isEmpty()) {
                // those names don exist any more.
                handle.createStatement(
                        "delete from workflows" +
                        " where id in (" +
                            oldNames.values().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")) + ")");
            }
        }

        @Override
        public void syncSchedulesToRevision(int repoId, List<Schedule> schedules)
            throws ResourceConflictException
        {
            if (schedules.isEmpty()) {
                return;
            }

            Map<String, Integer> oldNames = dao.getLatestScheduleNamesOfRepository(repoId);
            if (oldNames == null) {
                oldNames = ImmutableMap.of();
            }

            for (Schedule schedule : schedules) {
                if (oldNames.containsKey(schedule.getName())) {
                    // found the same name. overwriting source_id.
                    int n = handle.createStatement(
                            "update schedules" +
                            " set source_id = :sourceId, workflow_source_id = :workflowSourceId" +
                            // TODO should here update next_run_time nad next_schedule_time?
                            " where id = :id"
                        )
                        .bind("sourceId", schedule.getScheduleSourceId())
                        .bind("workflowSourceId", schedule.getWorkflowSourceId())
                        .bind("id", oldNames.get(schedule.getName()))
                        .execute();
                    if (n <= 0) {
                        // TODO exception?
                    }
                    oldNames.remove(schedule.getName());
                }
                else {
                    // not found this name. inserting new entry.
                    // TODO this INSERT can be optimized by using lazy multiple-value insert
                    catchConflict(() ->
                            handle.createStatement(
                                "insert into schedules" +
                                " (source_id, repository_id, name, workflow_source_id, next_run_time, next_schedule_time)" +
                                " values (:sourceId, :repoId, :name, :workflowSourceId, :nextRunTime, :nextScheduleTime)"
                            )
                            .bind("sourceId", schedule.getScheduleSourceId())
                            .bind("repoId", repoId)
                            .bind("name", schedule.getName())
                            .bind("workflowSourceId", schedule.getWorkflowSourceId())
                            .bind("nextRunTime", schedule.getNextRunTime().getTime() / 1000)
                            .bind("nextScheduleTime", schedule.getNextScheduleTime().getTime() / 1000)
                            .execute(),
                            "schedule name=%s", schedule.getName());
                }
            }
            if (!oldNames.isEmpty()) {
                // those names don exist any more.
                handle.createStatement(
                        "delete from schedules" +
                        " where id in (" +
                            oldNames.values().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")) + ")");
            }
        }
    }

    public interface Dao
    {
        @SqlQuery("select * from repositories" +
                " where site_id = :siteId" +
                " and id > :lastId" +
                " order by id asc" +
                " limit :limit")
        List<StoredRepository> getRepositories(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select * from repositories" +
                " where site_id = :siteId" +
                " and id = :id" +
                " limit 1")
        StoredRepository getRepositoryById(@Bind("siteId") int siteId, @Bind("id") int id);

        @SqlQuery("select * from repositories" +
                " where site_id = :siteId" +
                " and name = :name" +
                " limit 1")
        StoredRepository getRepositoryByName(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlUpdate("insert into repositories" +
                " (site_id, name, created_at, updated_at)" +
                " values (:siteId, :name, now(), now())")
        @GetGeneratedKeys
        int insertRepository(@Bind("siteId") int siteId, @Bind("name") String name);


        @SqlQuery("select t.* from revisions t" +
                " join repositories on repositories.id = t.repository_id" +
                " where t.site_id = :siteId" +
                " and t.repository_id = :repoId" +
                " and t.id > :lastId" +
                " order by t.id asc" +
                " limit :limit")
        List<StoredRevision> getRevisions(@Bind("siteId") int siteId, @Bind("repoId") int repoId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select t.* from revisions t" +
                " join repositories on repositories.id = t.repository_id" +
                " where site_id = :siteId" +
                " and t.id = :id" +
                " limit 1")
        StoredRevision getRevisionById(@Bind("siteId") int siteId, @Bind("id") int id);

        @SqlQuery("select t.* from revisions t" +
                " join repositories on repositories.id = t.repository_id" +
                " where site_id = :siteId" +
                " and t.repository_id = :repoId" +
                " and t.name = :name" +
                " limit 1")
        StoredRevision getRevisionByName(@Bind("siteId") int siteId, @Bind("repoId") int repoId, @Bind("name") String name);

        @SqlQuery("select t.* from revisions t" +
                " join repositories on repositories.id = t.repository_id" +
                " where site_id = :siteId" +
                " and t.repository_id = :repoId" +
                " order by t.id asc" +
                " limit 1")
        StoredRevision getLatestActiveRevision(@Bind("siteId") int siteId, @Bind("repoId") int repoId);

        @SqlUpdate("insert into revisions" +
                " (repository_id, name, default_params, archive_type, archive_md5, archive_path, archive_data, created_at)" +
                " values (:repoId, :name, :defaultParams, :archiveType, :archiveMd5, :archivePath, :archiveData, now())")
        @GetGeneratedKeys
        int insertRevision(@Bind("repoId") int repoId, @Bind("name") String name, @Bind("defaultParams") Config defaultParams, @Bind("archiveType") String archiveType, @Bind("archiveMd5") byte[] archiveMd5, @Bind("archivePath") String archivePath, @Bind("archiveData") byte[] archiveData);


        @SqlQuery("select w.* from workflow_sources w" +
                " join revisions rev on rev.id = w.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where w.revision_id = :revId" +
                " and w.id > :lastId" +
                " order by w.id asc" +
                " limit :limit")
        List<StoredWorkflowSource> getWorkflowSources(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select w.id, w.revision_id, w.name, w.config,"+
                " repo.id as repo_id, repo.site_id, repo.created_at as repo_created_at, repo.updated_at as repo_updated_at, repo.name as repo_name," +
                " rev.name as rev_name, rev.default_params as rev_default_params" +
                " from workflow_sources w" +
                " join revisions rev on w.revision_id = rev.id" +
                " join repositories repo on rev.repository_id = repo.id" +
                " where w.revision_id in (" +
                    "select max(rev.id) as rev_id" +
                    " from repositories repo" +
                    " join revisions rev on repo.id = rev.repository_id" +
                    " where repo.site_id = :siteId" +
                    " group by repo.id" +
                ")" +
                " and w.id > :lastId" +
                " order by w.id")
        List<StoredWorkflowSourceWithRepository> getLatestActiveWorkflowSources(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select w.id, w.revision_id, w.name, w.config,"+
                " repo.id as repo_id, repo.site_id, repo.created_at as repo_created_at, repo.updated_at as repo_updated_at, repo.name as repo_name," +
                " rev.name as rev_name, rev.default_params as rev_default_params" +
                " from workflow_sources w" +
                " join revisions rev on rev.id = w.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where w.id = :wfId")
        StoredWorkflowSourceWithRepository getWorkflowDetailsById(@Bind("wfId") int wfId);

        @SqlQuery("select w.* from workflow_sources w" +
                " join revisions rev on rev.id = w.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and w.id = :id" +
                " limit 1")
        StoredWorkflowSource getWorkflowSourceByid(@Bind("siteId") int siteId, @Bind("id") int id);

        @SqlQuery("select w.* from workflow_sources w" +
                " join revisions rev on rev.id = w.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and revision_id = :revId" +
                " and w.name = :name" +
                " limit 1")
        StoredWorkflowSource getWorkflowSourceByName(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("name") String name);

        @SqlQuery("select w.* from schedule_sources w" +
                " join revisions rev on rev.id = w.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and w.id = :id" +
                " limit 1")
        StoredScheduleSource getScheduleSourceByid(@Bind("siteId") int siteId, @Bind("id") int id);

        @SqlQuery("select w.* from schedule_sources w" +
                " join revisions rev on rev.id = w.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and revision_id = :revId" +
                " and w.name = :name" +
                " limit 1")
        StoredScheduleSource getScheduleSourceByName(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("name") String name);

        @SqlQuery("select w.* from workflow_sources w" +
                " join revisions rev on rev.id = w.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and w.revision_id = :revId" +
                " order by id asc" +
                " limit 1")
        StoredWorkflowSource getLatestActiveWorkflow(@Bind("siteId") int siteId, @Bind("revId") int revId);

        @SqlUpdate("insert into workflow_sources" +
                " (revision_id, name, config)" +
                " values (:revId, :name, :config)")
        @GetGeneratedKeys
        int insertWorkflowSource(@Bind("revId") int revId, @Bind("name") String name, @Bind("config") Config config);

        @SqlUpdate("insert into schedule_sources" +
                " (revision_id, name, config)" +
                " values (:revId, :name, :config)")
        @GetGeneratedKeys
        int insertScheduleSource(@Bind("revId") int revId, @Bind("name") String name, @Bind("config") Config config);

        @SqlQuery("select name, id from workflows" +
                " where repository_id = :repoId")
        Map<String, Integer> getLatestWorkflowNamesOfRepository(@Bind("repoId") int repoId);

        @SqlQuery("select name, id from schedules" +
                " where repository_id = :repoId")
        Map<String, Integer> getLatestScheduleNamesOfRepository(@Bind("repoId") int repoId);
    }

    private static class StoredRepositoryMapper
            implements ResultSetMapper<StoredRepository>
    {
        private final ConfigMapper cfm;

        public StoredRepositoryMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredRepository map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredRepository.builder()
                .id(r.getInt("id"))
                .siteId(r.getInt("site_id"))
                .createdAt(r.getTimestamp("created_at"))
                .updatedAt(r.getTimestamp("updated_at"))
                .name(r.getString("name"))
                .build();
        }
    }

    private static class StoredRevisionMapper
            implements ResultSetMapper<StoredRevision>
    {
        private final ConfigMapper cfm;

        public StoredRevisionMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredRevision map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredRevision.builder()
                .id(r.getInt("id"))
                .repositoryId(r.getInt("repository_id"))
                .createdAt(r.getTimestamp("created_at"))
                .name(r.getString("name"))
                .defaultParams(cfm.fromResultSetOrEmpty(r, "default_params"))
                .archiveType(r.getString("archive_type"))
                .archiveMd5(getOptionalBytes(r, "archive_md5"))
                .archivePath(getOptionalString(r, "archive_path"))
                .archiveData(getOptionalBytes(r, "archive_data"))
                .build();
        }
    }

    private static class StoredWorkflowSourceMapper
            implements ResultSetMapper<StoredWorkflowSource>
    {
        private final ConfigMapper cfm;

        public StoredWorkflowSourceMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredWorkflowSource map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredWorkflowSource.builder()
                .id(r.getInt("id"))
                .revisionId(r.getInt("revision_id"))
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .build();
        }
    }

    private static class StoredScheduleSourceMapper
            implements ResultSetMapper<StoredScheduleSource>
    {
        private final ConfigMapper cfm;

        public StoredScheduleSourceMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredScheduleSource map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredScheduleSource.builder()
                .id(r.getInt("id"))
                .revisionId(r.getInt("revision_id"))
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .build();
        }
    }

    private static class StoredWorkflowSourceWithRepositoryMapper
            implements ResultSetMapper<StoredWorkflowSourceWithRepository>
    {
        private final ConfigMapper cfm;

        public StoredWorkflowSourceWithRepositoryMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredWorkflowSourceWithRepository map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredWorkflowSourceWithRepository.builder()
                .id(r.getInt("id"))
                .revisionId(r.getInt("revision_id"))
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .repository(
                        ImmutableStoredRepository.builder()
                            .id(r.getInt("repo_id"))
                            .siteId(r.getInt("site_id"))
                            .createdAt(r.getTimestamp("repo_created_at"))
                            .updatedAt(r.getTimestamp("repo_updated_at"))
                            .name(r.getString("repo_name"))
                            .build())
                .revisionName("rev_name")
                .revisionDefaultParams(cfm.fromResultSetOrEmpty(r, "rev_default_params"))
                .build();
        }
    }
}
