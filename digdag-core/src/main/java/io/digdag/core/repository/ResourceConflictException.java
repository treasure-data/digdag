package io.digdag.core.repository;

/**
 * An exception thrown when unique identifier of a new resource already exists.
 *
 * This exception is deterministic.
 */
public class ResourceConflictException extends Exception
{
    public ResourceConflictException(String message)
    {
        super(message);
    }

    public ResourceConflictException(Throwable cause)
    {
        super(cause);
    }

    public ResourceConflictException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
