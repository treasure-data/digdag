package io.digdag.core.agent;

import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;

class DefaultTaskExecutionContext
        implements TaskExecutionContext
{
    private final PrivilegedVariables privilegedVariables;
    private final SecretProvider secretProvider;

    DefaultTaskExecutionContext(
            PrivilegedVariables privilegedVariables,
            SecretProvider secretProvider)
    {
        this.privilegedVariables = privilegedVariables;
        this.secretProvider = secretProvider;
    }

    @Override
    public PrivilegedVariables privilegedVariables()
    {
        return privilegedVariables;
    }

    @Override
    public SecretProvider secrets()
    {
        return secretProvider;
    }
}
