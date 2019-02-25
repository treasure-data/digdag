package io.digdag.standards.operator;

import io.digdag.spi.CommandRuntimeException;

public class RbRuntimeException
        extends CommandRuntimeException
{
    public RbRuntimeException(String message, String stacktrace)
    {
        super(message, stacktrace);
    }
}
