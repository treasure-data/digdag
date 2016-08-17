package io.digdag.spi;

public interface SecretControlStoreManager
{
    SecretControlStore getSecretControlStore(int siteId);
}
