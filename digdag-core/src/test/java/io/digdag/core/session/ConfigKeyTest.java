package io.digdag.core.session;

import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.is;


public class ConfigKeyTest
{
    @Test
    public void verifyParseAndToString()
    {
        assertReversible("a");
        assertReversible("foo.bar");
        assertReversible("foo.bar.baz");
    }

    @Test
    public void rejectInvalids()
    {
        assertInvalid("");
        assertInvalid(".");
        assertInvalid("a.");
        assertInvalid(".a");
        assertInvalid("a..");
        assertInvalid("a..b");
        assertInvalid(".x");
        assertInvalid("x.\"abc\".x");
    }

    private static void assertReversible(String key)
    {
        assertThat(ConfigKey.parse(key).toString(), is(key));
    }

    private static void assertInvalid(String key)
    {
        try {
            ConfigKey.parse(key);
            fail();
        }
        catch (IllegalArgumentException ex) {
            assertTrue(true);
        }
    }
}
