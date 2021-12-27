package io.digdag.core.database;

import com.google.inject.Inject;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;
import org.jdbi.v3.core.Jdbi;

public class DatabaseSecretStoreManager
        implements SecretStoreManager
{
    private final DatabaseConfig config;
    private final TransactionManager tm;
    private final ConfigMapper cfm;
    private final SecretCrypto crypto;

    @Inject
    public DatabaseSecretStoreManager(DatabaseConfig config, TransactionManager tm, ConfigMapper cfm, SecretCrypto crypto)
    {
        this.config = config;
        this.tm = tm;
        this.cfm = cfm;
        this.crypto = crypto;
    }

    @Override
    public SecretStore getSecretStore(int siteId)
    {
        return new DatabaseSecretStore(config, tm, cfm, siteId, crypto);
    }
}
