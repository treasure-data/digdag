package io.digdag.core.database;

import com.google.common.base.Optional;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.core.database.DatabaseSecretStore.EncryptedSecret;
import io.digdag.spi.SecretControlStore;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import static java.util.Locale.ENGLISH;

class DatabaseSecretControlStore
        extends BasicDatabaseStoreManager<DatabaseSecretControlStore.Dao>
        implements SecretControlStore
{
    private final int siteId;
    private final SecretCrypto crypto;

    DatabaseSecretControlStore(DatabaseConfig config, TransactionManager transactionManager, ConfigMapper cfm, int siteId, SecretCrypto crypto)
    {
        super(config.getType(), dao(config.getType()), transactionManager, cfm);
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
        transaction((handle, dao) -> {
            new LockedControl(handle, dao).setProjectSecret(projectId, scope, key, value);
            return null;
        });
    }

    @Override
    public void deleteProjectSecret(int projectId, String scope, String key)
    {
        transaction((handle, dao) -> {
            new LockedControl(handle, dao).deleteProjectSecret(projectId, scope, key);
            return null;
        });
    }

    @Override
    public List<String> listProjectSecrets(int projectId, String scope)
    {
        return transaction((handle, dao) -> {
            return new LockedControl(handle, dao).listProjectSecrets(projectId, scope);
        });
    }

    private class LockedControl
            implements SecretControlStore
    {
        private final Handle handle;
        private final Dao dao;

        public LockedControl(Handle handle, Dao dao)
        {
            this.handle = handle;
            this.dao = dao;
        }

        @Override
        public void setProjectSecret(int projectId, String scope, String key, String value)
        {
            String encrypted = crypto.encryptSecret(value);
            String engine = crypto.getName();

            dao.upsertProjectSecret(siteId, projectId, scope, key, engine, encrypted);
        }

        @Override
        public void deleteProjectSecret(int projectId, String scope, String key)
        {
            dao.deleteProjectSecret(siteId, projectId, scope, key);
        }

        @Override
        public List<String> listProjectSecrets(int projectId, String scope)
        {
            return dao.listProjectSecrets(siteId, projectId, scope);
        }

        @Override
        public <T> T lockProjectSecret(int projectId, String scope, String key, SecretLockAction<T> action)
        {
            EncryptedSecret secret = dao.lockProjectSecret(siteId, projectId, scope, key);

            // TODO this logic is copied from DatabaseSecretStore.getSecret.
            Optional<String> value;
            if (secret == null) {
                value = Optional.absent();
            }
            else {
                // TODO: look up crypto engine using name
                if (!crypto.getName().equals(secret.engine)) {
                    throw new AssertionError(String.format(ENGLISH,
                                "Crypto engine mismatch. Expected '%s' but got '%s'",
                                secret.engine, crypto.getName()));
                }
                String decrypted = crypto.decryptSecret(secret.value);
                value = Optional.of(decrypted);
            }

            return action.call(this, value);
        }
    }

    @Override
    public <T> T lockProjectSecret(int projectId, String scope, String key, SecretLockAction<T> action)
    {
        return transaction((handle, dao) -> {
            return new LockedControl(handle, dao).lockProjectSecret(projectId, scope, key, action);
        });
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

        @SqlQuery("select engine, value from secrets" +
                " where site_id = :siteId and project_id = :projectId and scope = :scope and key = :key" +
                " for update")
        EncryptedSecret lockProjectSecret(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope, @Bind("key") String key);
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
