package io.digdag.core.database;

import com.google.common.base.Strings;
import io.digdag.core.crypto.SecretCryptoException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Base64;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AESGCMSecretCryptoTest
{
    @Rule public ExpectedException expectedException = ExpectedException.none();

    private final static byte[] KEY1 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    private final static String KEY1_BASE64 = Base64.getEncoder().encodeToString(KEY1);

    private final static byte[] KEY2 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2};
    private final static String KEY2_BASE64 = Base64.getEncoder().encodeToString(KEY2);

    private final String V1_ENCRYPTED = "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";

    private AESGCMSecretCrypto crypto1;
    private AESGCMSecretCrypto crypto2;

    private static final String TEXT = "Hello Secret World!";

    @Before
    public void setUp()
            throws Exception
    {
        crypto1 = new AESGCMSecretCrypto(KEY1_BASE64);
        crypto2 = new AESGCMSecretCrypto(KEY2_BASE64);
    }

    @Test
    public void testDecryptV1()
            throws Exception
    {
        String decrypted = crypto1.decryptSecret(V1_ENCRYPTED);
        assertThat(decrypted, is(TEXT));
    }

    @Test
    public void testEncryptDecrypt()
            throws Exception
    {
        String encrypted = crypto1.encryptSecret(TEXT);
        String decrypted = crypto1.decryptSecret(encrypted);
        assertThat(decrypted, is(TEXT));
    }

    @Test
    public void verifyCannotDecryptWithWrongKey()
            throws Exception
    {
        String encrypted = crypto1.encryptSecret(TEXT);
        expectedException.expect(SecretCryptoException.class);
        crypto2.decryptSecret(encrypted);
    }

    @Test
    public void verifyTextSizeLimit()
            throws Exception
    {
        crypto1.encryptSecret(Strings.repeat(".", 16 * 1024));
        expectedException.expect(IllegalArgumentException.class);
        crypto1.encryptSecret(Strings.repeat(".", 16 * 1024 + 1));
    }
}
