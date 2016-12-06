package io.digdag.spi;

public interface TaskExecutionContext
{
    PrivilegedVariables privilegedVariables();

    SecretProvider secrets();
}
