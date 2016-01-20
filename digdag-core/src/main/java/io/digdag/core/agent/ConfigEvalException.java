package io.digdag.core.agent;

public class ConfigEvalException
    extends Exception
{
    public ConfigEvalException(String message)
    {
        super(message);
    }

    public ConfigEvalException(Throwable cause)
    {
        super(cause);
    }

    public ConfigEvalException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
