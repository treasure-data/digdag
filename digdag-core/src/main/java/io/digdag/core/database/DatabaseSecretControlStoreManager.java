package io.digdag.core.database;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.spi.SecretControlStore;
import io.digdag.spi.SecretControlStoreManager;
import org.skife.jdbi.v2.DBI;

public class DatabaseSecretControlStoreManager
        implements SecretControlStoreManager
{
    private final DatabaseConfig config;
    private final TransactionManager tm;
    private final ConfigMapper cfm;
    private final SecretCrypto crypto;

    @Inject
    public DatabaseSecretControlStoreManager(DatabaseConfig config, TransactionManager tm, ConfigMapper cfm, SecretCrypto crypto)
    {
        this.config = config;
        this.tm = tm;
        this.cfm = cfm;
        this.crypto = crypto;
    }

    @Override
    public SecretControlStore getSecretControlStore(int siteId)
    {
        return new DatabaseSecretControlStore(config, tm, cfm, siteId, crypto);
    }
}
