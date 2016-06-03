package io.digdag.core.workflow;

public class IllegalResumeException
        extends IllegalArgumentException
{
    public IllegalResumeException(String message)
    {
        super(message);
    }

    public IllegalResumeException(Throwable cause)
    {
        super(cause);
    }

    public IllegalResumeException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
