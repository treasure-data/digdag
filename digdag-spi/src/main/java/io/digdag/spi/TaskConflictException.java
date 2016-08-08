package io.digdag.spi;

public class TaskConflictException extends Exception
{
    public TaskConflictException(String message)
    {
        super(message);
    }

    public TaskConflictException(Throwable cause)
    {
        super(cause);
    }
}
