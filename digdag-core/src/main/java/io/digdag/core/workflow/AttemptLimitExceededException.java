package io.digdag.core.workflow;

import io.digdag.core.repository.ResourceLimitExceededException;

/**
 * An exception thrown when adding more attempts to a site than limit.
 *
 * Actual number of attempts is not guaranteed because limitation of number of
 * attempts is based on a slight old transaction.
 */
public class AttemptLimitExceededException
        extends ResourceLimitExceededException
{
    public AttemptLimitExceededException(String message)
    {
        super(message);
    }

    public AttemptLimitExceededException(Throwable cause)
    {
        super(cause);
    }

    public AttemptLimitExceededException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
