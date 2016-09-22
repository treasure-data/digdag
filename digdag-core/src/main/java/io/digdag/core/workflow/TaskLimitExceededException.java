package io.digdag.core.workflow;

/**
 * An exception thrown when adding more tasks to a session attempt would cause the task limit to be exceeded.
 *
 * Actual number of tasks is not guaranteed because limitation of number of
 * tasks is based on a slight old transaction.
 */
public class TaskLimitExceededException
        extends LimitExceededException
{
    // TODO (dano): should this be a checked exception?

    public TaskLimitExceededException(String message)
    {
        super(message);
    }

    public TaskLimitExceededException(Throwable cause)
    {
        super(cause);
    }

    public TaskLimitExceededException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
