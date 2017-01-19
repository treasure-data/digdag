package io.digdag.cli;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.Environment;
import io.digdag.spi.SecretStore;
import io.digdag.spi.SecretStoreManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

class LocalSecretStoreManager
        implements SecretStoreManager
{
    private final Map<String, String> secrets;
    private final Set<SecretStore> secretStores;
    private final Map<String, String> env;

    @Inject
    public LocalSecretStoreManager(Config systemConfig, Set<SecretStore> secretStores, @Environment Map<String, String> env)
    {
        this.secretStores = secretStores;
        this.env = env;
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

            // Read secrets from system config
            Optional<String> secret = Optional.fromNullable(secrets.get(key));
            if (secret.isPresent()) {
                return secret;
            }

            // Read secrets from local secret store
            Path secretsDir = ConfigUtil.digdagConfigHome(env).resolve("secrets");
            Path secretFilePath = secretsDir.resolve(key);
            try {
                byte[] bytes = Files.readAllBytes(secretFilePath);
                return Optional.of(new String(bytes, StandardCharsets.UTF_8));
            }
            catch (NoSuchFileException ignore) {
                return Optional.absent();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
