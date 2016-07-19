package io.digdag.core.agent;

import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;

class DefaultTaskExecutionContext
        implements TaskExecutionContext
{
    private SecretProvider secretProvider;

    DefaultTaskExecutionContext(SecretProvider secretProvider)
    {
        this.secretProvider = secretProvider;
    }

    @Override
    public SecretProvider secrets()
    {
        return secretProvider;
    }
}
