package io.digdag.spi;

public interface SecretStoreManager
{
    SecretStore getSecretStore(int siteId);
}
