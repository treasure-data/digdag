package io.digdag.cli;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;

import java.util.Map;

import static java.util.stream.Collectors.toMap;

class LocalSecretStoreManager
        implements SecretStoreManager
{
    private final Map<String, String> secrets;

    @Inject
    public LocalSecretStoreManager(Config systemConfig)
    {
        String prefix = "secrets.";
        this.secrets = systemConfig.getKeys().stream()
                .filter(k -> k.startsWith(prefix))
                .collect(toMap(
                        k -> k.substring(prefix.length(), k.length()),
                        k -> systemConfig.get(k, String.class)));
    }

    @Override
    public SecretStore getSecretStore(int siteId)
    {
        return (context, key) -> Optional.fromNullable(secrets.get(key));
    }
}
