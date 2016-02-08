package io.digdag.spi;

public class TemplateException
    extends Exception
{
    public TemplateException(String message)
    {
        super(message);
    }

    public TemplateException(Throwable cause)
    {
        super(cause);
    }

    public TemplateException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
