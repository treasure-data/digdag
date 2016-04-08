package io.digdag.core.database;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.immutables.value.Value;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.Schedule;
import io.digdag.client.api.IdName;
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
import static java.util.Locale.ENGLISH;
import static java.nio.charset.StandardCharsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;

public class DatabaseRepositoryStoreManager
        extends BasicDatabaseStoreManager<DatabaseRepositoryStoreManager.Dao>
        implements RepositoryStoreManager
{
    private final ConfigMapper cfm;

    @Inject
    public DatabaseRepositoryStoreManager(DBI dbi, ConfigMapper cfm, DatabaseConfig config)
    {
        super(config.getType(), Dao.class, dbi);

        dbi.registerMapper(new StoredRepositoryMapper(cfm));
        dbi.registerMapper(new StoredRevisionMapper(cfm));
        dbi.registerMapper(new StoredWorkflowDefinitionMapper(cfm));
        dbi.registerMapper(new StoredWorkflowDefinitionWithRepositoryMapper(cfm));
        dbi.registerMapper(new WorkflowConfigMapper());
        dbi.registerMapper(new IdNameMapper());
        dbi.registerArgumentFactory(cfm.getArgumentFactory());

        this.cfm = cfm;
    }

    @Override
    public RepositoryStore getRepositoryStore(int siteId)
    {
        return new DatabaseRepositoryStore(siteId);
    }

    @Override
    public StoredWorkflowDefinitionWithRepository getWorkflowDetailsById(long wfId)
            throws ResourceNotFoundException
    {
        return requiredResource(
                (handle, dao) -> dao.getWorkflowDetailsByIdInternal(wfId),
                "workflow id=%s", wfId);
    }

    @Override
    public StoredRepository getRepositoryByIdInternal(int repoId)
        throws ResourceNotFoundException
    {
        return requiredResource(
                (handle, dao) -> dao.getRepositoryByIdInternal(repoId),
                "repository id=%s", repoId);
    }

    @Override
    public StoredRevision getRevisionOfWorkflowDefinition(long wfId)
        throws ResourceNotFoundException
    {
        return requiredResource(
                (handle, dao) -> dao.getRevisionOfWorkflowDefinition(wfId),
                "revision of workflow definition id=%s", wfId);
    }

    private class DatabaseRepositoryStore
            implements RepositoryStore
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
            return autoCommit((handle, dao) -> dao.getRepositories(siteId, pageSize, lastId.or(0)));
        }

        @Override
        public RepositoryMap getRepositoriesByIdList(List<Integer> repoIdList)
        {
            if (repoIdList.isEmpty()) {
                return RepositoryMap.empty();
            }

            List<StoredRepository> repos = autoCommit((handle, dao) ->
                    handle.createQuery(
                        "select * from repositories" +
                        " where site_id = :siteId" +
                        " and id in (" +
                            repoIdList.stream()
                            .map(it -> Integer.toString(it)).collect(Collectors.joining(", ")) + ")"
                    )
                    .bind("siteId", siteId)
                    .map(new StoredRepositoryMapper(cfm))
                    .list()
                );

            ImmutableMap.Builder<Integer, StoredRepository> builder = ImmutableMap.builder();
            for (StoredRepository repo : repos) {
                builder.put(repo.getId(), repo);
            }
            return new RepositoryMap(builder.build());
        }

        @Override
        public StoredRepository getRepositoryById(int repoId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getRepositoryById(siteId, repoId),
                    "repository id=%d", repoId);
        }

        @Override
        public StoredRepository getRepositoryByName(String repoName)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getRepositoryByName(siteId, repoName),
                    "repository name=%s", repoName);
        }

        @Override
        public <T> T putAndLockRepository(Repository repository, RepositoryLockAction<T> func)
                throws ResourceConflictException
        {
            return transaction((handle, dao, ts) -> {
                int repoId;

                if (!ts.isRetried()) {
                    try {
                        repoId = catchConflict(() ->
                                dao.insertRepository(siteId, repository.getName()),
                                "repository name=%s", repository.getName());
                    }
                    catch (ResourceConflictException ex) {
                        ts.retry(ex);
                        return null;
                    }
                }
                else {
                    StoredRepository repo = dao.getRepositoryByName(siteId, repository.getName());
                    if (repo == null) {
                        throw new IllegalStateException("Database state error", ts.getLastException());
                    }
                    repoId = repo.getId();
                }

                StoredRepository repo = dao.lockRepository(repoId);
                if (repo == null) {
                    throw new IllegalStateException("Database state error");
                }

                return func.call(new DatabaseRepositoryControlStore(handle, siteId), repo);
            }, ResourceConflictException.class);
        }

        @Override
        public StoredRevision getRevisionById(int revId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getRevisionById(siteId, revId),
                    "revision id=%d", revId);
        }

        @Override
        public StoredRevision getRevisionByName(int repoId, String revName)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getRevisionByName(siteId, repoId, revName),
                    "revision name=%s in repository id=%d", revName, repoId);
        }

        @Override
        public StoredRevision getLatestRevision(int repoId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getLatestRevision(siteId, repoId),
                    "repository id=%d", repoId);
        }

        @Override
        public List<StoredRevision> getRevisions(int repoId, int pageSize, Optional<Integer> lastId)
        {
            return autoCommit((handle, dao) -> dao.getRevisions(siteId, repoId, pageSize, lastId.or(Integer.MAX_VALUE)));
        }

        @Override
        public byte[] getRevisionArchiveData(int revId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.selectRevisionArchiveData(revId),
                    "revisin id=%d", revId);
        }

        @Override
        public StoredWorkflowDefinitionWithRepository getLatestWorkflowDefinitionByName(int repoId, PackageName packageName, String name)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getLatestWorkflowDefinitionByName(siteId, repoId, packageName.getFullName(), name),
                    "workflow name=%s in the latest revision of repository id=%d", name, repoId);
        }

        @Override
        public List<StoredWorkflowDefinition> getWorkflowDefinitions(int revId, int pageSize, Optional<Long> lastId)
        {
            return autoCommit((handle, dao) -> dao.getWorkflowDefinitions(siteId, revId, pageSize, lastId.or(0L)));
        }

        @Override
        public StoredWorkflowDefinitionWithRepository getWorkflowDefinitionById(long wfId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getWorkflowDetailsById(siteId, wfId),
                    "workflow id=%d", wfId);
        }

        @Override
        public StoredWorkflowDefinition getWorkflowDefinitionByName(int revId, PackageName packageName, String name)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    (handle, dao) -> dao.getWorkflowDefinitionByName(siteId, revId, packageName.getFullName(), name),
                    "workflow name=%s in revision id=%d", name, revId);
        }

        @Override
        public TimeZoneMap getWorkflowTimeZonesByIdList(List<Long> defIdList)
        {
            if (defIdList.isEmpty()) {
                return TimeZoneMap.empty();
            }

            List<IdTimeZone> list = autoCommit((handle, dao) ->
                    handle.createQuery(
                        "select wd.id, wc.timezone from workflow_definitions wd" +
                        " join revisions rev on rev.id = wd.revision_id" +
                        " join repositories repo on repo.id = rev.repository_id" +
                        " join workflow_configs wc on wc.id = wd.config_id" +
                        " where wd.id in (" + defIdList.stream()
                            .map(it -> Long.toString(it)).collect(Collectors.joining(", ")) + ")" +
                        " and site_id = :siteId"
                    )
                    .bind("siteId", siteId)
                    .map(new IdTimeZoneMapper())
                    .list()
            );

            Map<Long, ZoneId> map = IdTimeZone.listToMap(list);
            return new TimeZoneMap(map);
        }
    }

    private static class IdTimeZone
    {
        protected final long id;
        protected final ZoneId timeZone;

        public IdTimeZone(long id, ZoneId timeZone)
        {
            this.id = id;
            this.timeZone = timeZone;
        }

        public static Map<Long, ZoneId> listToMap(List<IdTimeZone> list)
        {
            ImmutableMap.Builder<Long, ZoneId> builder = ImmutableMap.builder();
            for (IdTimeZone pair : list) {
                builder.put(pair.id, pair.timeZone);
            }
            return builder.build();
        }
    }

    private static class IdTimeZoneMapper
            implements ResultSetMapper<IdTimeZone>
    {
        @Override
        public IdTimeZone map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new IdTimeZone(r.getLong("id"), ZoneId.of(r.getString("timezone")));
        }
    }

    private class DatabaseRepositoryControlStore
            implements RepositoryControlStore
    {
        private final Handle handle;
        private final int siteId;
        private final Dao dao;

        public DatabaseRepositoryControlStore(Handle handle, int siteId)
        {
            this.handle = handle;
            this.siteId = siteId;
            this.dao = handle.attach(Dao.class);
        }

        /**
         * Create or overwrite a revision.
         *
         * This method doesn't check site id because RepositoryControl
         * interface is avaiable only if site is is valid.
         */
        @Override
        public StoredRevision insertRevision(int repoId, Revision revision)
            throws ResourceConflictException
        {
            int revId = catchConflict(() ->
                dao.insertRevision(repoId, revision.getName(), revision.getDefaultParams(), revision.getArchiveType(), revision.getArchiveMd5().orNull(), revision.getArchivePath().orNull()),
                "revision=%s in repository id=%d", revision.getName(), repoId);
            try {
                return requiredResource(
                        dao.getRevisionById(siteId, revId),
                        "revision id=%d", revId);
            }
            catch (ResourceNotFoundException ex) {
                throw new IllegalStateException("Database state error", ex);
            }
        }

        @Override
        public void insertRevisionArchiveData(int revId, byte[] data)
            throws ResourceConflictException
        {
            // TODO catch conflict and overwrite it
            catchConflict(() -> {
                    dao.insertRevisionArchiveData(revId, data);
                    return true;
                },
                "revision archive=%d", revId);
        }

        /**
         * Create a revision.
         *
         * This method doesn't check site id because RepositoryControl
         * interface is available only if site is is valid.
         */
        @Override
        public StoredWorkflowDefinition insertWorkflowDefinition(int repoId, int revId, WorkflowDefinition def, ZoneId workflowTimeZone)
            throws ResourceConflictException
        {
            String configText = cfm.toText(def.getConfig());
            String zoneId = workflowTimeZone.getId();
            long configDigest = WorkflowConfig.digest(configText, zoneId);

            int configId;

            WorkflowConfig found = dao.findWorkflowConfigByDigest(repoId, configDigest);
            if (found != null && WorkflowConfig.isEquivalent(found, configText, zoneId)) {
                configId = found.getId();
            }
            else {
                configId = dao.insertWorkflowConfig(repoId, configText, zoneId, configDigest);
            }

            long wfId = catchConflict(() ->
                dao.insertWorkflowDefinition(revId, def.getPackageName().getFullName(), def.getName(), configId),
                "workflow=%s in revision id=%d", def.getName(), revId);

            try {
                return requiredResource(
                        dao.getWorkflowDefinitionById(siteId, wfId),
                        "workflow id=%d", wfId);
            }
            catch (ResourceNotFoundException ex) {
                throw new IllegalStateException("Database state error", ex);
            }
        }

        @Override
        public void updateSchedules(int repoId, List<Schedule> schedules)
            throws ResourceConflictException
        {
            Map<String, Integer> oldNames = idNameListToHashMap(dao.getScheduleNames(repoId));

            for (Schedule schedule : schedules) {
                if (oldNames.containsKey(schedule.getWorkflowName())) {
                    // found the same name. overwriting workflow_definition_id
                    int n = handle.createStatement(
                            "update schedules" +
                            " set workflow_definition_id = :workflowDefinitionId, updated_at = now()" +
                            // TODO should here update next_run_time nad next_schedule_time?
                            " where id = :id"
                        )
                        .bind("workflowDefinitionId", schedule.getWorkflowDefinitionId())
                        .bind("id", oldNames.get(schedule.getWorkflowName()))
                        .execute();
                    if (n <= 0) {
                        // TODO exception?
                    }
                    oldNames.remove(schedule.getWorkflowName());
                }
                else {
                    // not found this name. inserting a new entry.
                    // TODO this INSERT can be optimized by using lazy multiple-value insert
                    catchConflict(() ->
                            handle.createStatement(
                                "insert into schedules" +
                                " (repository_id, workflow_definition_id, next_run_time, next_schedule_time, last_session_time, created_at, updated_at)" +
                                " values (:repoId, :workflowDefinitionId, :nextRunTime, :nextScheduleTime, NULL, now(), now())"
                            )
                            .bind("repoId", repoId)
                            .bind("workflowDefinitionId", schedule.getWorkflowDefinitionId())
                            .bind("nextRunTime", schedule.getNextRunTime().getEpochSecond())
                            .bind("nextScheduleTime", schedule.getNextScheduleTime().getEpochSecond())
                            .execute(),
                            "workflow_definition_id=%d", schedule.getWorkflowDefinitionId());
                }
            }
            if (!oldNames.isEmpty()) {
                // those names don exist any more.
                handle.createStatement(
                        "delete from schedules" +
                        " where id in (" +
                            oldNames.values().stream().map(it -> Integer.toString(it)).collect(Collectors.joining(", ")) + ")")
                    .execute();
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
                " and id = :id")
        StoredRepository getRepositoryById(@Bind("siteId") int siteId, @Bind("id") int id);

        @SqlQuery("select * from repositories" +
                " where id = :id")
        StoredRepository getRepositoryByIdInternal(@Bind("id") int id);

        @SqlQuery("select rev.*" +
                " from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " where wd.id = :id")
        StoredRevision getRevisionOfWorkflowDefinition(@Bind("id") long wfId);

        @SqlQuery("select * from repositories" +
                " where site_id = :siteId" +
                " and name = :name" +
                " limit 1")
        StoredRepository getRepositoryByName(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlQuery("select * from repositories where id = :id" +
                " for update")
        StoredRepository lockRepository(@Bind("id") int id);

        @SqlUpdate("insert into repositories" +
                " (site_id, name, created_at)" +
                " values (:siteId, :name, now())")
        @GetGeneratedKeys
        int insertRepository(@Bind("siteId") int siteId, @Bind("name") String name);

        @SqlQuery("select rev.* from revisions rev" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and rev.id = :id")
        StoredRevision getRevisionById(@Bind("siteId") int siteId, @Bind("id") int id);

        @SqlQuery("select rev.* from revisions rev" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and rev.repository_id = :repoId" +
                " and rev.name = :name" +
                " limit 1")
        StoredRevision getRevisionByName(@Bind("siteId") int siteId, @Bind("repoId") int repoId, @Bind("name") String name);

        @SqlQuery("select rev.* from revisions rev" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and rev.repository_id = :repoId" +
                " order by rev.id desc" +
                " limit 1")
        StoredRevision getLatestRevision(@Bind("siteId") int siteId, @Bind("repoId") int repoId);

        @SqlQuery("select rev.* from revisions rev" +
                " join repositories repo on repo.id = rev.repository_id" +
                " where site_id = :siteId" +
                " and rev.repository_id = :repoId" +
                " and rev.id < :lastId" +
                " order by rev.id desc" +
                " limit :limit")
        List<StoredRevision> getRevisions(@Bind("siteId") int siteId, @Bind("repoId") int repoId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select archive_data from revision_archives" +
                " where id = :revId")
        byte[] selectRevisionArchiveData(@Bind("revId") int revId);

        @SqlQuery("select wd.*, wc.config, wc.timezone," +
                " repo.id as repo_id, repo.name as repo_name, repo.site_id, repo.created_at as repo_created_at," +
                " rev.name as rev_name, rev.default_params as rev_default_params" +
                " from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.revision_id = (" +
                    "select max(id) from revisions" +
                    " where repository_id = :repoId" +
                ")" +
                " and wd.package_name = :packageName" +
                " and wd.name = :name" +
                " and repo.site_id = :siteId" +
                " limit 1")
        StoredWorkflowDefinitionWithRepository getLatestWorkflowDefinitionByName(@Bind("siteId") int siteId, @Bind("repoId") int repoId, @Bind("packageName") String packageName, @Bind("name") String name);

        // getWorkflowDetailsById is same with getWorkflowDetailsByIdInternal
        // excepting site_id check

        @SqlQuery("select wd.*, wc.config, wc.timezone," +
                " repo.id as repo_id, repo.name as repo_name, repo.site_id, repo.created_at as repo_created_at," +
                " rev.name as rev_name, rev.default_params as rev_default_params" +
                " from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.id = :id")
        StoredWorkflowDefinitionWithRepository getWorkflowDetailsByIdInternal(@Bind("id") long id);

        @SqlQuery("select wd.*, wc.config, wc.timezone," +
                " repo.id as repo_id, repo.name as repo_name, repo.site_id, repo.created_at as repo_created_at," +
                " rev.name as rev_name, rev.default_params as rev_default_params" +
                " from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.id = :id" +
                " and site_id = :siteId")
        StoredWorkflowDefinitionWithRepository getWorkflowDetailsById(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select wd.*, wc.config, wc.timezone from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.id = :id" +
                " and site_id = :siteId")
        StoredWorkflowDefinition getWorkflowDefinitionById(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select wd.*, wc.config, wc.timezone from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where revision_id = :revId" +
                " and wd.package_name = :packageName" +
                " and wd.name = :name" +
                " and site_id = :siteId" +
                " limit 1")
        StoredWorkflowDefinition getWorkflowDefinitionByName(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("packageName") String packageName, @Bind("name") String name);

        @SqlQuery("select id, config, timezone" +
                " from workflow_configs" +
                " where repository_id = :repoId and config_digest = :configDigest")
        WorkflowConfig findWorkflowConfigByDigest(@Bind("repoId") int repoId, @Bind("configDigest") long configDigest);

        @SqlUpdate("insert into workflow_configs" +
                " (repository_id, config, timezone, config_digest)" +
                " values (:repoId, :config, :timezone, :configDigest)")
        @GetGeneratedKeys
        int insertWorkflowConfig(@Bind("repoId") int repoId, @Bind("config") String config, @Bind("timezone") String timezone, @Bind("configDigest") long configDigest);

        @SqlUpdate("insert into revisions" +
                " (repository_id, name, default_params, archive_type, archive_md5, archive_path, created_at)" +
                " values (:repoId, :name, :defaultParams, :archiveType, :archiveMd5, :archivePath, now())")
        @GetGeneratedKeys
        int insertRevision(@Bind("repoId") int repoId, @Bind("name") String name, @Bind("defaultParams") Config defaultParams, @Bind("archiveType") String archiveType, @Bind("archiveMd5") byte[] archiveMd5, @Bind("archivePath") String archivePath);

        @SqlQuery("select wd.*, wc.config, wc.timezone from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.revision_id = :revId" +
                " and wd.id > :lastId" +
                " and repo.site_id = :siteId" +
                " order by wd.id asc" +
                " limit :limit")
        List<StoredWorkflowDefinition> getWorkflowDefinitions(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("limit") int limit, @Bind("lastId") long lastId);

        @SqlUpdate("insert into revision_archives" +
                " (id, archive_data)" +
                " values (:revId, :data)")
        void insertRevisionArchiveData(@Bind("revId") int revId, @Bind("data") byte[] data);

        @SqlUpdate("insert into workflow_definitions" +
                " (revision_id, package_name, name, config_id)" +
                " values (:revId, :packageName, :name, :configId)")
        @GetGeneratedKeys
        long insertWorkflowDefinition(@Bind("revId") int revId, @Bind("packageName") String packageName, @Bind("name") String name, @Bind("configId") int configId);

        @SqlQuery("select wd.name, schedules.id from schedules" +
                " join workflow_definitions wd on wd.id = schedules.workflow_definition_id" +
                " where schedules.repository_id = :repoId")
        List<IdName> getScheduleNames(@Bind("repoId") int repoId);
    }

    @Value.Immutable
    public static abstract class WorkflowConfig
    {
        public abstract int getId();

        public abstract String getConfigText();

        public abstract String getTimeZone();

        private static final MessageDigest md5;

        static {
            try {
                md5 = MessageDigest.getInstance("MD5");
            }
            catch (NoSuchAlgorithmException ex) {
                throw new RuntimeException(ex);
            }
        }

        public static long digest(String configText, String zoneId)
        {
            try {
                String target = configText + " " + zoneId;
                byte[] digest = ((MessageDigest) md5.clone()).digest(target.getBytes(UTF_8));
                return ByteBuffer.wrap(digest).getLong(0);
            }
            catch (CloneNotSupportedException ex) {
                throw new RuntimeException(ex);
            }
        }

        public static boolean isEquivalent(WorkflowConfig c, String configText, String zoneId)
        {
            return configText.equals(c.getConfigText()) && zoneId.equals(c.getTimeZone());
        }
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
                .name(r.getString("name"))
                .siteId(r.getInt("site_id"))
                .createdAt(getTimestampInstant(r, "created_at"))
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
                .createdAt(getTimestampInstant(r, "created_at"))
                .name(r.getString("name"))
                .defaultParams(cfm.fromResultSetOrEmpty(r, "default_params"))
                .archiveType(r.getString("archive_type"))
                .archiveMd5(getOptionalBytes(r, "archive_md5"))
                .archivePath(getOptionalString(r, "archive_path"))
                .build();
        }
    }

    private static class StoredWorkflowDefinitionMapper
            implements ResultSetMapper<StoredWorkflowDefinition>
    {
        private final ConfigMapper cfm;

        public StoredWorkflowDefinitionMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredWorkflowDefinition map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredWorkflowDefinition.builder()
                .id(r.getLong("id"))
                .revisionId(r.getInt("revision_id"))
                .timeZone(ZoneId.of(r.getString("timezone")))
                .packageName(PackageName.of(r.getString("package_name")))
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .build();
        }
    }

    private static class StoredWorkflowDefinitionWithRepositoryMapper
            implements ResultSetMapper<StoredWorkflowDefinitionWithRepository>
    {
        private final ConfigMapper cfm;

        public StoredWorkflowDefinitionWithRepositoryMapper(ConfigMapper cfm)
        {
            this.cfm = cfm;
        }

        @Override
        public StoredWorkflowDefinitionWithRepository map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableStoredWorkflowDefinitionWithRepository.builder()
                .id(r.getLong("id"))
                .revisionId(r.getInt("revision_id"))
                .timeZone(ZoneId.of(r.getString("timezone")))
                .packageName(PackageName.of(r.getString("package_name")))
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .repository(
                        ImmutableStoredRepository.builder()
                            .id(r.getInt("repo_id"))
                            .name(r.getString("repo_name"))
                            .siteId(r.getInt("site_id"))
                            .createdAt(getTimestampInstant(r, "repo_created_at"))
                            .build())
                .revisionName(r.getString("rev_name"))
                .revisionDefaultParams(cfm.fromResultSetOrEmpty(r, "rev_default_params"))
                .build();
        }
    }

    private static class WorkflowConfigMapper
            implements ResultSetMapper<WorkflowConfig>
    {
        @Override
        public WorkflowConfig map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return ImmutableWorkflowConfig.builder()
                .id(r.getInt("id"))
                .configText(r.getString("config"))
                .timeZone(r.getString("timezone"))
                .build();
        }
    }

    private static class IdNameMapper
            implements ResultSetMapper<IdName>
    {
        @Override
        public IdName map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return IdName.of(r.getInt("id"), r.getString("name"));
        }
    }

    private HashMap<String, Integer> idNameListToHashMap(List<IdName> list)
    {
        HashMap<String, Integer> map = new HashMap<>();
        for (IdName idName : list) {
            map.put(idName.getName(), idName.getId());
        }
        return map;
    }
}
