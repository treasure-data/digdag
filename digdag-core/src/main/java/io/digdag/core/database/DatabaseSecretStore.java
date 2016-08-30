package io.digdag.core.database;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretAccessDeniedException;
import io.digdag.spi.SecretScopes;
import io.digdag.spi.SecretStore;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

class DatabaseSecretStore
        extends BasicDatabaseStoreManager<DatabaseSecretStore.Dao>
        implements SecretStore
{
    private static final Map<String, Integer> PRIORITIES = ImmutableMap.of(
            SecretScopes.PROJECT, 0,
            SecretScopes.PROJECT_DEFAULT, 1);

    private final int siteId;

    private final SecretCrypto crypto;

    DatabaseSecretStore(DatabaseConfig config, DBI dbi, int siteId, SecretCrypto crypto)
    {
        super(config.getType(), Dao.class, dbi);
        this.siteId = siteId;
        this.crypto = crypto;
        dbi.registerMapper(new ScopedSecretMapper());
    }

    @Override
    public Optional<String> getSecret(SecretAccessContext context, String key)
    {
        if (context.siteId() != siteId) {
            throw new SecretAccessDeniedException("Site id mismatch");
        }

        Optional<Integer> projectId = context.projectId();
        if (!projectId.isPresent()) {
            return Optional.absent();
        }

        List<ScopedSecret> secrets = autoCommit((handle, dao) -> dao.getProjectSecrets(siteId, projectId.get(), key));

        if (secrets.isEmpty()) {
            return Optional.absent();
        }

        ScopedSecret secret = secrets.stream()
                .filter(s -> PRIORITIES.containsKey(s.scope))
                .sorted((a, b) -> PRIORITIES.get(a.scope) - PRIORITIES.get(b.scope))
                .findFirst().orElseThrow(AssertionError::new);
        String decrypted = decrypt(secret);

        return Optional.of(decrypted);
    }

    @Override
    public Optional<String> getSecret(SecretAccessContext context, String scope, String key)
    {
        if (context.siteId() != siteId) {
            throw new SecretAccessDeniedException("Site id mismatch");
        }

        Optional<Integer> projectId = context.projectId();
        if (!projectId.isPresent()) {
            return Optional.absent();
        }

        ScopedSecret secret = autoCommit((handle, dao) -> dao.getProjectSecret(siteId, projectId.get(), scope, key));

        if (secret == null) {
            return Optional.absent();
        }

        String decrypted = decrypt(secret);

        return Optional.of(decrypted);
    }

    private String decrypt(ScopedSecret secret)
    {
        // TODO: look up crypto engine using name
        if (!crypto.getName().equals(secret.engine)) {
            throw new AssertionError("Crypto engine mismatch");
        }

        return crypto.decryptSecret(secret.value);
    }

    interface Dao
    {
        @SqlQuery("select scope, engine, value from secrets" +
                " where site_id = :siteId and project_id = :projectId and key = :key")
        List<ScopedSecret> getProjectSecrets(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("key") String key);

        @SqlQuery("select scope, engine, value from secrets" +
                " where site_id = :siteId and project_id = :projectId and key = :key and scope = :scope")
        ScopedSecret getProjectSecret(@Bind("siteId") int siteId, @Bind("projectId") int projectId, @Bind("scope") String scope, @Bind("key") String key);
    }

    private static class ScopedSecret
    {
        private final String scope;
        private final String engine;
        private final String value;

        private ScopedSecret(String scope, String engine, String value)
        {
            this.scope = scope;
            this.engine = engine;
            this.value = value;
        }
    }

    private class ScopedSecretMapper
            implements ResultSetMapper<ScopedSecret>
    {
        @Override
        public ScopedSecret map(int index, ResultSet r, StatementContext ctx)
                throws SQLException
        {
            return new ScopedSecret(r.getString("scope"), r.getString("engine"), r.getString("value"));
        }
    }
}
