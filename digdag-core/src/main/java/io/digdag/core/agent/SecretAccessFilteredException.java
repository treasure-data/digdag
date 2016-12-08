package io.digdag.core.agent;

import io.digdag.spi.SecretAccessDeniedException;

class SecretAccessFilteredException
        extends SecretAccessDeniedException
{
    public SecretAccessFilteredException(String key, String message)
    {
        super(key, message);
    }
}
