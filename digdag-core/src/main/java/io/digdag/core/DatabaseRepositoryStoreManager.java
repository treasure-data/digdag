package io.digdag.core;

import java.util.List;
import java.util.Date;
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

public class DatabaseRepositoryStoreManager
        extends BasicDatabaseStoreManager
        implements RepositoryStoreManager
{
    private final Handle handle;
    private final Dao dao;

    @Inject
    public DatabaseRepositoryStoreManager(IDBI dbi, ConfigSourceMapper cfm)
    {
        this.handle = dbi.open();
        handle.registerMapper(new StoredRepositoryMapper(cfm));
        handle.registerMapper(new StoredRevisionMapper(cfm));
        handle.registerMapper(new StoredWorkflowSourceMapper(cfm));
        handle.registerArgumentFactory(cfm.getArgumentFactory());
        this.dao = handle.attach(Dao.class);
    }

    public RepositoryStore getRepositoryStore(int siteId)
    {
        return new DatabaseRepositoryStore(siteId);
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

        public <T> T transaction(StoreTransaction<T> transaction)
        {
            return handle.inTransaction((handle, session) -> transaction.call());
        }

        public List<StoredRepository> getAllRepositories()
        {
            return dao.getRepositories(siteId, Integer.MAX_VALUE, 0);
        }

        public List<StoredRepository> getRepositories(int pageSize, Optional<Integer> lastId)
        {
            return dao.getRepositories(siteId, pageSize, lastId.or(0));
        }

        public StoredRepository getRepositoryById(int repoId)
        {
            return dao.getRepositoryById(siteId, repoId);
        }

        public StoredRepository getRepositoryByName(String name)
        {
            return dao.getRepositoryByName(siteId, name);
        }

        public <T> T putRepository(Repository repository, RepositoryLockAction<T> func)
        {
            return handle.inTransaction((handle, session) -> {
                // TODO idempotent operation
                int repoId = dao.insertRepository(siteId, repository.getName());
                RepositoryControl control = new RepositoryControl(this, getRepositoryById(repoId));
                return func.call(control);
            });
        }

        public void deleteRepository(int repoId)
        {
            throw new UnsupportedOperationException("not implemented yet");
        }

        public List<StoredRevision> getAllRevisions(int repoId)
        {
            return dao.getRevisions(siteId, repoId, Integer.MAX_VALUE, 0);
        }

        public List<StoredRevision> getRevisions(int repoId, int pageSize, Optional<Integer> lastId)
        {
            return dao.getRevisions(siteId, repoId, pageSize, lastId.or(0));
        }

        public StoredRevision getRevisionById(int revId)
        {
            return dao.getRevisionById(siteId, revId);
        }

        public StoredRevision getRevisionByName(int repoId, String name)
        {
            return dao.getRevisionByName(siteId, repoId, name);
        }

        public StoredRevision getLatestActiveRevision(int repoId)
        {
            return dao.getLatestActiveRevision(siteId, repoId);
        }

        public StoredRevision putRevision(int repoId, Revision revision)
        {
            // TODO idempotent operation
            // TODO check site id
            int revId = dao.insertRevision(repoId, revision.getName(), revision.getGlobalParams(), revision.getArchiveType(), revision.getArchiveMd5().orNull(), revision.getArchivePath().orNull(), revision.getArchiveData().orNull());
            return getRevisionById(revId);
        }

        public void deleteRevision(int revId)
        {
            throw new UnsupportedOperationException("not implemented yet");
        }


        public List<StoredWorkflowSource> getAllWorkflows(int revId)
        {
            return dao.getWorkflows(siteId, revId, Integer.MAX_VALUE, 0);
        }

        public List<StoredWorkflowSource> getWorkflows(int revId, int pageSize, Optional<Integer> lastId)
        {
            return dao.getWorkflows(siteId, revId, pageSize, lastId.or(0));
        }

        public List<StoredWorkflowSourceWithRepository> getAllLatestActiveWorkflows()
        {
            throw new UnsupportedOperationException("not implemented yet");
        }

        public StoredWorkflowSource getWorkflowById(int wfId)
        {
            return dao.getWorkflowById(siteId, wfId);
        }

        public StoredWorkflowSource getWorkflowByName(int revId, String name)
        {
            return dao.getWorkflowByName(siteId, revId, name);
        }

        public StoredWorkflowSource putWorkflow(int revId, WorkflowSource workflow)
        {
            // TODO idempotent operation
            // TODO check site id
            int wfId = dao.insertWorkflow(revId, workflow.getName(), workflow.getConfig());
            return getWorkflowById(wfId);
        }

        public void deleteWorkflow(int wfId)
        {
            throw new UnsupportedOperationException("not implemented yet");
        }
    }

    public interface Dao
    {
        @SqlQuery("select * from repositories" +
                " where site_id = :siteId" +
                " and id > :lastId" +
                " order by id desc" +
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
                " order by t.id desc" +
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
                " order by t.id desc" +
                " limit 1")
        StoredRevision getLatestActiveRevision(@Bind("siteId") int siteId, @Bind("repoId") int repoId);

        @SqlUpdate("insert into revisions" +
                " (repository_id, name, global_params, archive_type, archive_md5, archive_path, archive_data, created_at)" +
                " values (:repoId, :name, :globalParams, :archiveType, :archiveMd5, :archivePath, :archiveData, now())")
        @GetGeneratedKeys
        int insertRevision(@Bind("repoId") int repoId, @Bind("name") String name, @Bind("globalParams") ConfigSource globalParams, @Bind("archiveType") String archiveType, @Bind("archiveMd5") byte[] archiveMd5, @Bind("archivePath") String archivePath, @Bind("archiveData") byte[] archiveData);


        @SqlQuery("select t.* from workflows t" +
                " join revisions on revisions.id = t.revision_id" +
                " join repositories on repositories.id = revisions.repository_id" +
                " and t.revision_id = :revId" +
                " and t.id > :lastId" +
                " order by t.id desc" +
                " limit :limit")
        List<StoredWorkflowSource> getWorkflows(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("limit") int limit, @Bind("lastId") int lastId);

        @SqlQuery("select t.* from workflows t" +
                " join revisions on revisions.id = t.revision_id" +
                " join repositories on repositories.id = revisions.repository_id" +
                " where site_id = :siteId" +
                " and t.id = :id" +
                " limit 1")
        StoredWorkflowSource getWorkflowById(@Bind("siteId") int siteId, @Bind("id") int id);

        @SqlQuery("select t.* from workflows t" +
                " join revisions on revisions.id = t.revision_id" +
                " join repositories on repositories.id = revisions.repository_id" +
                " where site_id = :siteId" +
                " and revision_id = :revId" +
                " and name = :name" +
                " limit 1")
        StoredWorkflowSource getWorkflowByName(@Bind("siteId") int siteId, @Bind("revId") int revId, @Bind("name") String name);

        @SqlQuery("select t.* from workflows t" +
                " join revisions on revisions.id = t.revision_id" +
                " join repositories on repositories.id = revisions.repository_id" +
                " where site_id = :siteId" +
                " and t.revision_id = :revId" +
                " order by id desc" +
                " limit 1")
        StoredWorkflowSource getLatestActiveWorkflow(@Bind("siteId") int siteId, @Bind("revId") int revId);

        @SqlUpdate("insert into workflows" +
                " (revision_id, name, config)" +
                " values (:revId, :name, :config)")
        @GetGeneratedKeys
        int insertWorkflow(@Bind("revId") int revId, @Bind("name") String name, @Bind("config") ConfigSource config);
    }

    private static class StoredRepositoryMapper
            implements ResultSetMapper<StoredRepository>
    {
        private final ConfigSourceMapper cfm;

        public StoredRepositoryMapper(ConfigSourceMapper cfm)
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
                //.latestRevisionId(r.getInt("latest_revision_id"))
                .name(r.getString("name"))
                //.config(cfm.fromResultSetOrEmpty(r, "config"))
                .build();
        }
    }

    private static class StoredRevisionMapper
            implements ResultSetMapper<StoredRevision>
    {
        private final ConfigSourceMapper cfm;

        public StoredRevisionMapper(ConfigSourceMapper cfm)
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
                .globalParams(cfm.fromResultSetOrEmpty(r, "global_params"))
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
        private final ConfigSourceMapper cfm;

        public StoredWorkflowSourceMapper(ConfigSourceMapper cfm)
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
}
