package io.digdag.spi;

public class NotificationException
        extends Exception
{
    public NotificationException(String message)
    {
        super(message);
    }

    public NotificationException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
