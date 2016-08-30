package io.digdag.cli;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;

import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

class LocalSecretStoreManager
        implements SecretStoreManager
{
    private final Map<String, String> secrets;
    private final Set<SecretStore> secretStores;

    @Inject
    public LocalSecretStoreManager(Config systemConfig, Set<SecretStore> secretStores)
    {
        this.secretStores = secretStores;
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
        return (projectId, scope, key) -> {

            // First attempt to find a matching secret in the backing stores
            for (SecretStore store : secretStores) {
                Optional<String> secret = store.getSecret(projectId, scope, key);
                if (secret.isPresent()) {
                    return secret;
                }
            }

            // Fall back to secrets from system config
            return Optional.fromNullable(secrets.get(key));
        };
    }
}
