package io.digdag.core.database;

import com.google.common.base.Preconditions;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.core.crypto.SecretCrypto;
import io.digdag.core.crypto.SecretCryptoException;

import javax.crypto.AEADBadTagException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.crypto.Cipher.ENCRYPT_MODE;

/**
 * A {@link SecretCrypto} implementation that encrypts secrets using AES in GCM mode, padding up to the GCM tag length to obfuscate the actual size of the secret.
 *
 * <p>The encrypted record format is (offsets in bytes):</p>
 *
 * <pre>
 * 0          6         10     14      26                  
 * +-----------------------------------------------------------------+
 * | "aesgcm" | version | term | nonce | encrypted payload | gcm tag |
 * +-----------------------------------------------------------------+
 *      6B        4B       4B      12B         n x 16B         16B
 * </pre>
 *
 * <p>The encrypted payload contains a 32 bit length integer and ((n x 16) - 4) Bytes of text + padding. Both the length and the text + padding is encrypted.</p>
 *
 */
public class AESGCMSecretCrypto implements SecretCrypto
{
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final String NAME = "aesgcm";
    private static final byte[] NAME_BYTES = NAME.getBytes(UTF_8);

    private final SecretKey sharedSecret;

    private static final int AES_KEY_SIZE = 128;
    private static final int GCM_NONCE_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 16;

    private static final int TERM = 1;
    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;

    private static final int RECORD_SIZE_ALIGNMENT = GCM_TAG_LENGTH;

    private static final int MAX_PLAINTEXT_LENGTH = 16 * 1024;

    private static final int LENGTH_SIZE = 4;
    private static final int NAME_SIZE = NAME_BYTES.length;
    private static final int TERM_SIZE = 4;
    private static final int VERSION_SIZE = 4;
    private static final int WRAPPING_SIZE =
            NAME_SIZE + VERSION_SIZE + TERM_SIZE + GCM_NONCE_LENGTH;

    public AESGCMSecretCrypto(String sharedSecretBase64)
    {
        byte[] sharedSecretRaw = Base64.getDecoder().decode(sharedSecretBase64);
        Preconditions.checkArgument(sharedSecretRaw.length * 8 == AES_KEY_SIZE);
        this.sharedSecret = new SecretKeySpec(sharedSecretRaw, "AES");
    }

    @Override
    public String encryptSecret(String plainText)
    {
        if (plainText.length() > MAX_PLAINTEXT_LENGTH) {
            throw new IllegalArgumentException("Too long text");
        }

        byte[] nonce = generateNonce();

        Cipher cipher = cipher(ENCRYPT_MODE, sharedSecret, nonce);

        byte[] plainTextBytes = plainText.getBytes(UTF_8);

        if (plainTextBytes.length > MAX_PLAINTEXT_LENGTH) {
            throw new IllegalArgumentException("Too long text");
        }

        int recordContentsLength = LENGTH_SIZE + plainTextBytes.length;

        int recordLength = RECORD_SIZE_ALIGNMENT * ((recordContentsLength + RECORD_SIZE_ALIGNMENT - 1) / RECORD_SIZE_ALIGNMENT);

        byte[] recordBytes = new byte[recordLength];
        ByteBuffer recordBuffer = ByteBuffer.wrap(recordBytes);
        recordBuffer.putInt(plainTextBytes.length);
        recordBuffer.put(plainTextBytes);

        byte[] cipherText;
        try {
            cipherText = cipher.doFinal(recordBytes);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw ThrowablesUtil.propagate(e);
        }

        byte[] opaque = new byte[WRAPPING_SIZE + cipherText.length];
        ByteBuffer output = ByteBuffer.wrap(opaque);

        output.put(NAME_BYTES);
        output.putInt(VERSION_2);
        output.putInt(TERM);
        output.put(nonce);
        output.put(cipherText);

        assert output.remaining() == 0;

        return Base64.getEncoder().encodeToString(opaque);
    }

    @Override
    public String decryptSecret(String encryptedBase64)
    {
        byte[] encrypted = Base64.getDecoder().decode(encryptedBase64);
        Preconditions.checkArgument(encrypted.length >= WRAPPING_SIZE + GCM_TAG_LENGTH, "Bad size");

        ByteBuffer buffer = ByteBuffer.wrap(encrypted);

        byte[] nameBytes = new byte[NAME_SIZE];
        buffer.get(nameBytes);
        if (!Arrays.equals(NAME_BYTES, nameBytes)) {
            throw new IllegalArgumentException("Crypto engine mismatch");
        }

        int version = buffer.getInt();
        if (version != VERSION_1 && version != VERSION_2) {
            throw new IllegalArgumentException("Bad version");
        }

        int term = buffer.getInt();
        if (term != TERM) {
            throw new IllegalArgumentException("Bad term");
        }

        byte[] nonce = new byte[GCM_NONCE_LENGTH];
        buffer.get(nonce);

        Cipher cipher = cipher(Cipher.DECRYPT_MODE, sharedSecret, nonce);

        byte[] recordBytes;
        try {
            recordBytes = cipher.doFinal(encrypted, buffer.position(), buffer.remaining());
        }
        catch (AEADBadTagException e) {
            throw new SecretCryptoException(e);
        }
        catch (IllegalBlockSizeException | BadPaddingException e) {
            throw ThrowablesUtil.propagate(e);
        }

        ByteBuffer decryptedBuffer = ByteBuffer.wrap(recordBytes);
        int length = decryptedBuffer.getInt();
        if (length < -1 || length > MAX_PLAINTEXT_LENGTH) {
            throw new IllegalArgumentException("Bad length");
        }

        decryptedBuffer.limit(decryptedBuffer.position() + length);

        return UTF_8.decode(decryptedBuffer).toString();
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    private Cipher cipher(int encryptMode, SecretKey sharedSecret, byte[] nonce)
    {
        Cipher result;
        try {
            result = Cipher.getInstance("AES/GCM/NoPadding");
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e1) {
            throw ThrowablesUtil.propagate(e1);
        }
        Cipher cipher = result;

        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, nonce);
        try {
            cipher.init(encryptMode, sharedSecret, spec);
        }
        catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw ThrowablesUtil.propagate(e);
        }
        return cipher;
    }

    private byte[] generateNonce()
    {
        // The nonce need not be random, just unique. It is simply convenient to rely on
        // SecureRandom (/dev/urandom) to give us a value which is very likely to (although
        // of course not guaranteed) be unique.
        byte[] nonce = new byte[GCM_NONCE_LENGTH];
        SECURE_RANDOM.nextBytes(nonce);
        return nonce;
    }
}
