package io.digdag.core.database;

import com.google.common.base.Optional;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.spi.SecretStore;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

class DatabaseSecretStore
        extends BasicDatabaseStoreManager<DatabaseSecretStore.Dao>
        implements SecretStore
{
    private final int siteId;

    private final SecretCrypto crypto;

    DatabaseSecretStore(DatabaseConfig config, TransactionManager transactionManager, ConfigMapper cfm, int siteId, SecretCrypto crypto)
    {
        super(config.getType(), Dao.class, transactionManager, cfm);
        this.siteId = siteId;
        this.crypto = crypto;
    }

    @Override
    public Optional<String> getSecret(int projectId, String scope, String key)
    {
        EncryptedSecret secret = autoCommit((handle, dao) -> dao.getProjectSecret(siteId, projectId, scope, key));

        if (secret == null) {
            return Optional.absent();
        }

        // TODO: look up crypto engine using name
        if (!crypto.getName().equals(secret.engine)) {
            throw new AssertionError("Crypto engine mismatch");
        }

        String decrypted = crypto.decryptSecret(secret.value);

        return Optional.of(decrypted);
    }

    interface Dao
    {
        @SqlQuery("select engine, value from secrets" +
                " where site_id = :siteId and project_id = :projectId and key = :key and scope = :scope")
        EncryptedSecret getProjectSecret(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope, @Bind("key") String key);
    }

    private static class EncryptedSecret
    {
        private final String engine;
        private final String value;

        private EncryptedSecret(String engine, String value)
        {
            this.engine = engine;
            this.value = value;
        }
    }

    static class ScopedSecretMapper
            implements ResultSetMapper<EncryptedSecret>
    {
        @Override
        public EncryptedSecret map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new EncryptedSecret(r.getString("engine"), r.getString("value"));
        }
    }
}
