package io.digdag.core.workflow;

public abstract class LimitExceededException
        extends RuntimeException
{
    public LimitExceededException(String message)
    {
        super(message);
    }

    public LimitExceededException(Throwable cause)
    {
        super(cause);
    }

    public LimitExceededException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
