package io.digdag.spi;

public class SecretAccessDeniedException
        extends RuntimeException
{
    private String key;

    public SecretAccessDeniedException(String key, String message)
    {
        super(message);
        this.key = key;
    }

    public String getKey()
    {
        return key;
    }
}
