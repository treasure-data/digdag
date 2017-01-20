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

    DatabaseSecretControlStore(DatabaseConfig config, TransactionManager transactionManager, int siteId, SecretCrypto crypto)
    {
        super(config.getType(), dao(config.getType()), transactionManager);
        this.siteId = siteId;
        this.crypto = crypto;
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
    public void setProjectSecret(int projectId, String scope, String key, String value)
    {
        String encrypted = crypto.encryptSecret(value);
        String engine = crypto.getName();

        transaction((handle, dao) -> {
            dao.upsertProjectSecret(siteId, projectId, scope, key, engine, encrypted);
            return null;
        });
    }

    @Override
    public void deleteProjectSecret(int projectId, String scope, String key)
    {
        transaction((handle, dao) -> {
            dao.deleteProjectSecret(siteId, projectId, scope, key);
            return null;
        });
    }

    @Override
    public List<String> listProjectSecrets(int projectId, String scope)
    {
        return transaction((handle, dao) -> dao.listProjectSecrets(siteId, projectId, scope));
    }

    interface Dao
    {
        @SqlQuery("select key from secrets" +
                " where site_id = :siteId and project_id = :projectId and scope = :scope")
        List<String> listProjectSecrets(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope);

        @SqlUpdate("delete from secrets" +
                " where site_id = :siteId and project_id = :projectId and scope = :scope and key = :key")
        int deleteProjectSecret(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope, @Bind("key") String key);

        int upsertProjectSecret(int siteId, int projectId, String scope, String key, String engine, String value);
    }

    interface PgDao
            extends Dao
    {
        @Override
        @SqlUpdate("insert into secrets" +
                " (site_id, project_id, scope, key, engine, value, updated_at)" +
                " values (:siteId, :projectId, :scope, :key, :engine, :value, now())" +
                " on conflict (site_id, project_id, scope, key) do update set engine = :engine, value = :value, updated_at = now()")
        int upsertProjectSecret(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope, @Bind("key") String key, @Bind("engine") String engine, @Bind("value") String value);
    }

    interface H2Dao
            extends Dao
    {
        @Override
        @SqlUpdate("merge into secrets " +
                " (site_id, project_id, scope, key, engine, value, updated_at)" +
                " key(site_id, project_id, scope, key)" +
                " values (:siteId, :projectId, :scope, :key, :engine, :value, now())"
        )
        int upsertProjectSecret(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope, @Bind("key") String key, @Bind("engine") String engine, @Bind("value") String value);
    }
}
