package io.digdag.core;

import io.digdag.spi.SecretAccessContext;
import io.digdag.spi.SecretAccessPolicy;

public class LocalSecretAccessPolicy
        implements SecretAccessPolicy
{
    @Override
    public boolean isSecretAccessible(SecretAccessContext context, String key)
    {
        // TODO: Restrict default access in local mode?
        return true;
    }
}
