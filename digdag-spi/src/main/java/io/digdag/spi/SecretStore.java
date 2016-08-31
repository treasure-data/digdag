package io.digdag.spi;

import com.google.common.base.Optional;

public interface SecretStore
{
    Optional<String> getSecret(int projectId, String scope, String key);
}
