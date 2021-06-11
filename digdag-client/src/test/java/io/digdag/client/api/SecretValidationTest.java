package io.digdag.client.api;

import com.google.common.base.Strings;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class SecretValidationTest
{
    @Test
    public void isValidSecretKey()
            throws Exception
    {
        assertThat(SecretValidation.isValidSecretKey(""), is(false));
        assertThat(SecretValidation.isValidSecretKey("."), is(false));
        assertThat(SecretValidation.isValidSecretKey("0"), is(false));
        assertThat(SecretValidation.isValidSecretKey("0.foo"), is(false));
        assertThat(SecretValidation.isValidSecretKey("0234"), is(false));
        assertThat(SecretValidation.isValidSecretKey("0bar"), is(false));
        assertThat(SecretValidation.isValidSecretKey("012bar"), is(false));
        assertThat(SecretValidation.isValidSecretKey("foo.0"), is(false));
        assertThat(SecretValidation.isValidSecretKey("foo.1"), is(false));
        assertThat(SecretValidation.isValidSecretKey("foo.1.baz"), is(false));
        assertThat(SecretValidation.isValidSecretKey("foo..baz"), is(false));
        assertThat(SecretValidation.isValidSecretKey("\u2603"), is(false));
        assertThat(SecretValidation.isValidSecretKey("\uD83D\uDCA9"), is(false));
        assertThat(SecretValidation.isValidSecretKey("r\u00e4ksm\u00f6rg\u00e5s"), is(false));

        assertThat(SecretValidation.isValidSecretKey("f"), is(true));
        assertThat(SecretValidation.isValidSecretKey("foo"), is(true));
        assertThat(SecretValidation.isValidSecretKey("foo.b"), is(true));
        assertThat(SecretValidation.isValidSecretKey("foo.bar"), is(true));
        assertThat(SecretValidation.isValidSecretKey("foo.bar.baz"), is(true));
        assertThat(SecretValidation.isValidSecretKey("a1.b2c.d3"), is(true));
    }

    @Test
    public void isValidSecretValue()
            throws Exception
    {
        assertThat(SecretValidation.isValidSecretValue(""), is(true));
        assertThat(SecretValidation.isValidSecretValue("foobar"), is(true));
        assertThat(SecretValidation.isValidSecretValue("\uD83D\uDCA9"), is(true));
        assertThat(SecretValidation.isValidSecretValue("r\u00e4ksm\u00f6rg\u00e5s"), is(true));
        assertThat(SecretValidation.isValidSecretValue(Strings.repeat(".", 16 * 1024)), is(true));

        assertThat(SecretValidation.isValidSecretValue(Strings.repeat(".", 16 * 1024 + 1)), is(false));
        assertThat(SecretValidation.isValidSecretValue(Strings.repeat("\u2603", 16 * 1024 / 3 + 1)), is(false));
    }
}
