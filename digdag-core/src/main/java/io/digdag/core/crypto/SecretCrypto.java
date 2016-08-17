package io.digdag.core.crypto;

public interface SecretCrypto
{
    /**
     * Encrypt a secret value.
     * @param plainText The value to encrypt.
     * @return A base64 encoded encrypted secret.
     */
    String encryptSecret(String plainText);

    /**
     * Decrypt an encrypted secret.
     * @param encryptedBase64 A base64 encoded encrypted secret to decrypt.
     * @return The decrypted secret in plain text.
     * @throws SecretCryptoException if the secret could not be decrypted due to e.g. crypto key mismatch or data corruption.
     */
    String decryptSecret(String encryptedBase64);

    /**
     * Get the name of this crypto implementation.
     */
    String getName();
}
