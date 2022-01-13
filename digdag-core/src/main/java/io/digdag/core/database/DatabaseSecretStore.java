package io.digdag.core.database;

import com.google.common.base.Optional;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.spi.SecretStore;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.core.mapper.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import static java.util.Locale.ENGLISH;

class DatabaseSecretStore
        extends BasicDatabaseStoreManager<DatabaseSecretStore.Dao>
        implements SecretStore
{
    private final int siteId;
    private final TransactionManager tm;
    private final SecretCrypto crypto;

    DatabaseSecretStore(DatabaseConfig config, TransactionManager tm, ConfigMapper cfm, int siteId, SecretCrypto crypto)
    {
        super(config.getType(), Dao.class, tm, cfm);
        this.siteId = siteId;
        this.tm = tm;
        this.crypto = crypto;
    }

    @Override
    public Optional<String> getSecret(int projectId, String scope, String key)
    {
        EncryptedSecret secret =
                tm.begin(() -> autoCommit((handle, dao) -> dao.getProjectSecret(siteId, projectId, scope, key)));

        if (secret == null) {
            return Optional.absent();
        }

        // TODO: look up crypto engine using name
        if (!crypto.getName().equals(secret.engine)) {
            throw new AssertionError(String.format(ENGLISH,
                        "Crypto engine mismatch. Expected '%s' but got '%s'",
                        secret.engine, crypto.getName()));
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

    static class EncryptedSecret
    {
        final String engine;
        final String value;

        private EncryptedSecret(String engine, String value)
        {
            this.engine = engine;
            this.value = value;
        }
    }

    static class ScopedSecretMapper
            implements RowMapper<EncryptedSecret>
    {
        @Override
        public EncryptedSecret map(ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new EncryptedSecret(r.getString("engine"), r.getString("value"));
        }
    }
}
