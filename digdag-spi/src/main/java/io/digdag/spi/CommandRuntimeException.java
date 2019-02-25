package io.digdag.spi;

public class CommandRuntimeException
        extends RuntimeException
{
    private final String stacktrace;

    public CommandRuntimeException(String message, String stacktrace)
    {
        super(message);
        this.stacktrace = stacktrace;
    }

    public String getStacktrace()
    {
        return this.stacktrace;
    }
}
