package io.digdag.spi;

/**
 * An exception thrown for secret accesses that would be accessible under the
 * access policies in effect but for which a matching secret could not be found.
 */
public class SecretNotFoundException
        extends RuntimeException
{
    private final String key;

    public SecretNotFoundException(String key)
    {
        super("Secret not found for key: '" + key + "'");
        this.key = key;
    }

    public String getKey()
    {
        return key;
    }
}
