package io.digdag.core.database;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.sql.Timestamp;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.immutables.value.Value;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.digdag.core.repository.*;
import io.digdag.core.schedule.Schedule;
import io.digdag.client.api.IdName;
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
import static com.google.common.base.Preconditions.checkArgument;

public class DatabaseRepositoryStoreManager
        extends BasicDatabaseStoreManager
        implements RepositoryStoreManager
{
    private final Handle handle;
    private final Dao dao;
    private final ConfigMapper cfm;

    @Inject
    public DatabaseRepositoryStoreManager(IDBI dbi, ConfigMapper cfm)
    {
        this.handle = dbi.open();
        this.cfm = cfm;
        handle.registerMapper(new StoredRepositoryMapper(cfm));
        handle.registerMapper(new StoredRevisionMapper(cfm));
        handle.registerMapper(new StoredWorkflowDefinitionMapper(cfm));
        handle.registerMapper(new StoredWorkflowDefinitionWithRepositoryMapper(cfm));
        handle.registerMapper(new WorkflowConfigMapper());
        handle.registerMapper(new IdNameMapper());
        handle.registerArgumentFactory(cfm.getArgumentFactory());
        this.dao = handle.attach(Dao.class);
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
                dao.getWorkflowDetailsById(wfId),
                "workflow id=%s", wfId);
    }

    @Override
    public StoredRepository getRepositoryByIdInternal(int repoId)
        throws ResourceNotFoundException
    {
        return requiredResource(
                dao.getRepositoryByIdInternal(repoId),
                "repository id=%s", repoId);
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
        public <T> T putAndLockRepository(Repository repository, RepositoryLockAction<T> func)
        {
            // TODO this code should use MERGE (h2) or INSERT ... ON CONFLICT (PostgreSQL)
            return handle.inTransaction((handle, session) -> {
                int repoId;
                try {
                    repoId = catchConflict(() ->
                            dao.insertRepository(siteId, repository.getName()),
                            "repository name=%s", repository.getName());
                }
                catch (ResourceConflictException ex) {
                    StoredRepository repo = dao.getRepositoryByName(siteId, repository.getName());
                    if (repo == null) {
                        throw new IllegalStateException("Database state error", ex);
                    }
                    repoId = repo.getId();
                }

                StoredRepository repo = dao.lockRepository(repoId);
                if (repo == null) {
                    throw new IllegalStateException("Database state error");
                }

                return func.call(this, repo);
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
        public StoredRevision getLatestRevision(int repoId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getLatestRevision(siteId, repoId),
                    "repository id=%d", repoId);
        }

        @Override
        public byte[] getRevisionArchiveData(int revId)
                throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.selectRevisionArchiveData(revId),
                    "revisin id=%d", revId);
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
                        dao.insertRevision(repoId, revision.getName(), revision.getDefaultParams(), revision.getArchiveType(), revision.getArchiveMd5().orNull(), revision.getArchivePath().orNull()),
                        "revision=%s in repository id=%d", revision.getName(), repoId);
                    return getRevisionById(revId);
                }
                catch (ResourceConflictException ex) {
                    // TODO delete archive data first?
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

        @Override
        public List<StoredWorkflowDefinition> getWorkflowDefinitions(int revId, int pageSize, Optional<Integer> lastId)
        {
            return dao.getWorkflowDefinitions(siteId, revId, pageSize, lastId.or(0));
        }

        @Override
        public StoredWorkflowDefinitionWithRepository getLatestWorkflowDefinitionByName(int repoId, String name)
            throws ResourceNotFoundException
        {
            return dao.getLatestWorkflowDefinitionByName(siteId, repoId, name);
        }

        @Override
        public List<StoredWorkflowDefinitionWithRepository> getLatestWorkflowDefinitions(int pageSize, Optional<Integer> lastId)
        {
            return dao.getLatestWorkflowDefinitions(siteId, pageSize, lastId.or(0));
        }

        @Override
        public StoredWorkflowDefinition getWorkflowDefinitionById(long wfId)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getWorkflowDefinitionById(siteId, wfId),
                    "workflow id=%d", wfId);
        }

        @Override
        public StoredWorkflowDefinition getWorkflowDefinitionByName(int revId, String name)
            throws ResourceNotFoundException
        {
            return requiredResource(
                    dao.getWorkflowDefinitionByName(siteId, revId, name),
                    "workflow name=%s in revision id=%d", name, revId);
        }

        /**
         * Create a revision.
         *
         * This method doesn't check site id because RepositoryControl
         * interface is avaiable only if site is is valid.
         */
        @Override
        public StoredWorkflowDefinition insertWorkflowDefinition(int repoId, int revId, WorkflowDefinition def)
            throws ResourceConflictException
        {
            try {
                String text = cfm.toText(def.getConfig());
                long configDigest = cfm.toConfigDigest(text);

                int configId;
                WorkflowConfig found = dao.findWorkflowConfigByDigest(repoId, configDigest);
                if (found != null && found.getConfig().equals(def.getConfig())) {
                    configId = found.getId();
                }
                else {
                    configId = dao.insertWorkflowConfig(repoId, text, configDigest);
                }

                long wfId = catchConflict(() ->
                    dao.insertWorkflowDefinition(revId, def.getName(), configId),
                    "workflow=%s in revision id=%d", def.getName(), revId);
                return getWorkflowDefinitionById(wfId);
            }
            catch (ResourceNotFoundException ex) {
                throw new IllegalStateException("Database state error", ex);
            }
        }

        @Override
        public void updateSchedules(int repoId, List<Schedule> schedules)
            throws ResourceConflictException
        {
            if (schedules.isEmpty()) {
                return;
            }

            Map<String, Long> oldNames = idNameListToHashMap(dao.getScheduleNames(repoId));
            if (oldNames == null) {
                oldNames = ImmutableMap.of();
            }

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
                                " (repository_id, workflow_definition_id, next_run_time, next_schedule_time, created_at, last_session_instant, updated_at)" +
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
                            oldNames.values().stream().map(it -> Long.toString(it)).collect(Collectors.joining(", ")) + ")");
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
                " and t.id = :id")
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
        StoredRevision getLatestRevision(@Bind("siteId") int siteId, @Bind("repoId") int repoId);

        @SqlUpdate("insert into revisions" +
                " (repository_id, name, default_params, archive_type, archive_md5, archive_path, created_at)" +
                " values (:repoId, :name, :defaultParams, :archiveType, :archiveMd5, :archivePath, now())")
        @GetGeneratedKeys
        int insertRevision(@Bind("repoId") int repoId, @Bind("name") String name, @Bind("defaultParams") Config defaultParams, @Bind("archiveType") String archiveType, @Bind("archiveMd5") byte[] archiveMd5, @Bind("archivePath") String archivePath);

        @SqlUpdate("insert into revision_archives" +
                " (id, data)" +
                " values (:revId, :data)")
        void insertRevisionArchiveData(@Bind("revId") int revId, @Bind("data") byte[] data);

        @SqlQuery("select data from revision_archives" +
                " where id = :revId")
        byte[] selectRevisionArchiveData(@Bind("revId") int revId);

        @SqlQuery("select wd.*, wc.config from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.revision_id = :revId" +
                " and wd.id > :lastId" +
                " and repo.site_id = :siteId" +
                " order by wd.id asc" +
                " limit :limit")
        List<StoredWorkflowDefinition> getWorkflowDefinitions(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select wd.*, wc.config from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.revision_id = (" +
                    "select max(id) from revisions" +
                    " where repository_id = :repoId" +
                ")" +
                " and wd.name = :name" +
                " and repo.site_id = :siteId" +
                " limit 1")
        StoredWorkflowDefinitionWithRepository getLatestWorkflowDefinitionByName(@Bind("siteId") int siteId, @Bind("repoId") int repoId, @Bind("name") String name);

        @SqlQuery("select wd.*, wc.config from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.revision_id = (" +
                    "select max(id) from revisions" +
                    " where repository_id = :repoId" +
                ")" +
                " and repo.site_id = :siteId" +
                " order by wd.id desc")
        List<StoredWorkflowDefinitionWithRepository> getLatestWorkflowDefinitions(@Bind("siteId") int siteId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select wd.id, wd.revision_id, wd.name, wc.config,"+
                " repo.id as repo_id, repo.site_id, repo.created_at as repo_created_at, repo.updated_at as repo_updated_at, repo.name as repo_name," +
                " rev.name as rev_name, rev.default_params as rev_default_params" +
                " from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.id = :wfId")
        StoredWorkflowDefinitionWithRepository getWorkflowDetailsById(@Bind("wfId") long wfId);

        @SqlQuery("select wd.*, wc.config from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where wd.id = :id" +
                " and site_id = :siteId")
        StoredWorkflowDefinition getWorkflowDefinitionById(@Bind("siteId") int siteId, @Bind("id") long id);

        @SqlQuery("select wd.* from workflow_definitions wd" +
                " join revisions rev on rev.id = wd.revision_id" +
                " join repositories repo on repo.id = rev.repository_id" +
                " join workflow_configs wc on wc.id = wd.config_id" +
                " where revision_id = :revId" +
                " and wd.name = :name" +
                " and site_id = :siteId" +
                " limit 1")
        StoredWorkflowDefinition getWorkflowDefinitionByName(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("name") String name);

        @SqlQuery("select id, config" +
                " from workflow_configs" +
                " where repository_id = :repoId and config_digest = :configDigest")
        WorkflowConfig findWorkflowConfigByDigest(@Bind("repoId") int repoId, @Bind("configDigest") long configDigest);

        @SqlUpdate("insert into workflow_configs" +
                " (repository_id, config, config_digest)" +
                " values (:repoId, :config, :configDigest)")
        @GetGeneratedKeys
        int insertWorkflowConfig(@Bind("repoId") int repoId, @Bind("config") String config, @Bind("configDigest") long configDigest);

        @SqlUpdate("insert into workflow_definitions" +
                " (revision_id, name, config_id)" +
                " values (:revId, :name, :configId)")
        @GetGeneratedKeys
        int insertWorkflowDefinition(@Bind("revId") int revId, @Bind("name") String name, @Bind("configId") int configId);

        @SqlQuery("select wd.name, schedules.id from schedules" +
                " join workflow_definitions wd on wd.id = schedules.workflow_definition_id" +
                " where schedules.repository_id = :repoId")
        List<IdName> getScheduleNames(@Bind("repoId") int repoId);
    }

    @Value.Immutable
    static abstract class WorkflowConfig
    {
        public abstract int getId();

        public abstract String getConfig();
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
                .name(r.getString("name"))
                .config(cfm.fromResultSetOrEmpty(r, "config"))
                .repository(
                        ImmutableStoredRepository.builder()
                            .id(r.getInt("repo_id"))
                            .name(r.getString("repo_name"))
                            .siteId(r.getInt("site_id"))
                            .createdAt(getTimestampInstant(r, "repo_created_at"))
                            .build())
                .revisionName("rev_name")
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
                .config(r.getString("config"))
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
            return IdName.of(r.getLong("id"), r.getString("name"));
        }
    }

    private HashMap<String, Long> idNameListToHashMap(List<IdName> list)
    {
        if (list == null) {
            return null;
        }
        HashMap<String, Long> map = new HashMap<>();
        for (IdName idName : list) {
            map.put(idName.getName(), idName.getId());
        }
        return map;
    }
}
