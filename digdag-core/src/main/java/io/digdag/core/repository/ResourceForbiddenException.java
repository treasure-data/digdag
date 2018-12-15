package io.digdag.core.repository;

/**
 * An exception thrown when a required resource (project, revision, session, etc.) does not be permitted by an user.
 *
 * This exception is deterministic.
 */
public class ResourceForbiddenException extends Exception
{
    public ResourceForbiddenException(String message)
    {
        super(message);
    }

    public ResourceForbiddenException(Throwable cause)
    {
        super(cause);
    }
}
