package io.digdag.spi;

public class TaskStateException extends RuntimeException
{
    public TaskStateException(String message)
    {
        super(message);
    }

    public TaskStateException(Throwable cause)
    {
        super(cause);
    }
}

