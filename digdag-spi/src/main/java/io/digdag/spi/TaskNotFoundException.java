package io.digdag.spi;

public class TaskNotFoundException extends Exception
{
    public TaskNotFoundException(String message)
    {
        super(message);
    }

    public TaskNotFoundException(Throwable cause)
    {
        super(cause);
    }
}
