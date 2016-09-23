package io.digdag.core.repository;

/**
 * An exception thrown when number of certain resources exceeds limit.
 *
 * This exception is deterministic unless some resources are removed.
 *
 * Actual number of attempts is not guaranteed because limitation of number of
 * attempts may be based on a slight old transaction.
 */
public abstract class ResourceLimitExceededException
        extends Exception
{
    public ResourceLimitExceededException(String message)
    {
        super(message);
    }

    public ResourceLimitExceededException(Throwable cause)
    {
        super(cause);
    }

    public ResourceLimitExceededException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
