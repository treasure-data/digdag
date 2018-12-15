package io.digdag.spi;

/**
 * An exception thrown when a required resource (project, revision, session, etc.) does not be permitted by an user.
 *
 * This exception is deterministic.
 */
public class AccessControlException
        extends Exception
{
    public AccessControlException(String message)
    {
        super(message);
    }

    public AccessControlException(Throwable cause)
    {
        super(cause);
    }
}
