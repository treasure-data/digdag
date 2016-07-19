package io.digdag.core.database;

import com.google.inject.Inject;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;
import org.skife.jdbi.v2.DBI;

public class DatabaseSecretStoreManager
        implements SecretStoreManager
{
    private final DatabaseConfig config;
    private final DBI dbi;
    private final SecretCrypto crypto;

    @Inject
    public DatabaseSecretStoreManager(DatabaseConfig config, DBI dbi, SecretCrypto crypto)
    {
        this.config = config;
        this.dbi = dbi;
        this.crypto = crypto;
    }

    @Override
    public SecretStore getSecretStore(int siteId)
    {
        return new DatabaseSecretStore(config, dbi, siteId, crypto);
    }
}
