package io.digdag.core.database;

import com.google.common.base.Strings;
import io.digdag.core.crypto.SecretCryptoException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class AESGCMSecretCryptoTest
{
    @Rule public ExpectedException expectedException = ExpectedException.none();

    private final static byte[] KEY1 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    private final static String KEY1_BASE64 = Base64.getEncoder().encodeToString(KEY1);

    private final static byte[] KEY2 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2};
    private final static String KEY2_BASE64 = Base64.getEncoder().encodeToString(KEY2);

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
        crypto1.encryptSecret(Strings.repeat(".", 1024));
        expectedException.expect(IllegalArgumentException.class);
        crypto1.encryptSecret(Strings.repeat(".", 1024 + 1));
    }
}
