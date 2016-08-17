package io.digdag.spi;

public class SecretAccessDeniedException
        extends RuntimeException
{
    private String key;

    public SecretAccessDeniedException(String key)
    {
        super("Access denied for key: '" + key + "'");
        this.key = key;
    }

    public String getKey()
    {
        return key;
    }
}
