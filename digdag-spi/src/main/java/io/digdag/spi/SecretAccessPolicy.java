package io.digdag.spi;

public interface SecretAccessPolicy
{
    boolean isSecretAccessible(SecretAccessContext context, String key);
}
