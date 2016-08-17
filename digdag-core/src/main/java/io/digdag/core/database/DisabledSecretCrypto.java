package io.digdag.core.database;

import io.digdag.core.crypto.SecretCrypto;

public class DisabledSecretCrypto
        implements SecretCrypto
{
    @Override
    public String encryptSecret(String plainText)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String decryptSecret(String encryptedBase64)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName()
    {
        return "disabled";
    }
}
