package io.digdag.core.repository;

/**
 * An exception thrown when a required resource (project, revision, session, etc.) does not exist.
 *
 * This exception is deterministic.
 */
public class ResourceNotFoundException extends Exception
{
    public ResourceNotFoundException(String message)
    {
        super(message);
    }

    public ResourceNotFoundException(Throwable cause)
    {
        super(cause);
    }
}

