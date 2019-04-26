package io.digdag.spi.ac;

/**
 * An exception thrown when a required action to a target (project, revision, session, etc.) is not be permitted.
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
