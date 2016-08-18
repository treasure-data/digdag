package io.digdag.spi;

import com.google.common.base.Optional;

public interface SecretStore
{
    Optional<String> getSecret(SecretAccessContext context, String key);
}
