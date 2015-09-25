package io.digdag.core;

public class ConfigException
        extends RuntimeException
{
    protected ConfigException()
    {
        super();
    }

    public ConfigException(String message)
    {
        super(message);
    }

    public ConfigException(Throwable cause)
    {
        super(cause);
    }

    public ConfigException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
