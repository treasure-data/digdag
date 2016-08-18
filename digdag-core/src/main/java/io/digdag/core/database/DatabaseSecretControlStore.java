package io.digdag.core.database;

import io.digdag.core.crypto.SecretCrypto;
import io.digdag.spi.SecretControlStore;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;

import java.util.List;

class DatabaseSecretControlStore
        extends BasicDatabaseStoreManager<DatabaseSecretControlStore.Dao>
        implements SecretControlStore
{
    private final int siteId;
    private final SecretCrypto crypto;

    DatabaseSecretControlStore(DatabaseConfig config, DBI dbi, int siteId, SecretCrypto crypto)
    {
        super(config.getType(), Dao.class, dbi);
        this.siteId = siteId;
        this.crypto = crypto;
    }

    @Override
    public void setProjectSecret(int projectId, String scope, String key, String value)
    {
        String encrypted = crypto.encryptSecret(value);
        String engine = crypto.getName();

        transaction((handle, dao, ts) -> {
            dao.deleteProjectSecret(siteId, projectId, scope, key);
            dao.insertProjectSecret(siteId, projectId, scope, key, engine, encrypted);
            return null;
        });
    }

    @Override
    public void deleteProjectSecret(int projectId, String scope, String key)
    {
        transaction((handle, dao, ts) -> {
            dao.deleteProjectSecret(siteId, projectId, scope, key);
            return null;
        });
    }

    @Override
    public List<String> listProjectSecrets(int projectId, String scope)
    {
        return transaction((handle, dao, ts) -> dao.listProjectSecrets(siteId, projectId, scope));
    }

    interface Dao
    {
        @SqlQuery("select key from secrets" +
                " where site_id = :siteId and project_id = :projectId and scope = :scope")
        List<String> listProjectSecrets(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope);

        @SqlUpdate("delete from secrets" +
                " where site_id = :siteId and project_id = :projectId and scope = :scope and key = :key")
        int deleteProjectSecret(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope, @Bind("key") String key);

        @SqlUpdate("insert into secrets" +
                " (site_id, project_id, scope, key, engine, value, updated_at)" +
                " values (:siteId, :projectId, :scope, :key, :engine, :value, now())")
        int insertProjectSecret(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope, @Bind("key") String key, @Bind("engine") String engine, @Bind("value") String value);
    }
}
