package io.digdag.core.agent;

import io.digdag.spi.SecretSelector;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class SecretSelectorTest
{
    @Test
    public void match()
            throws Exception
    {
        assertThat(SecretSelector.of("*").match("foo"), is(true));
        assertThat(SecretSelector.of("*").match("bar"), is(true));
        assertThat(SecretSelector.of("foo").match("foo"), is(true));
        assertThat(SecretSelector.of("foo").match("bar"), is(false));
        assertThat(SecretSelector.of("foo.*").match("foo.bar"), is(true));
        assertThat(SecretSelector.of("foo.*").match("foo"), is(false));
        assertThat(SecretSelector.of("foo.*").match("bar"), is(false));
    }

    @Test
    public void testValidation()
            throws Exception
    {
        assertValidSelector("*");
        assertValidSelector("f.*");
        assertValidSelector("foo.*");
        assertInvalidSelector("");
        assertInvalidSelector(".");
        assertInvalidSelector(".*");
        assertInvalidSelector("foo.bar*");
        assertInvalidSelector("foo.b*");
    }

    private void assertInvalidSelector(String s)
    {
        try {
            SecretSelector.of(s);
            fail();
        } catch (IllegalArgumentException ignore) {
        }
    }

    private void assertValidSelector(String s)
    {
        SecretSelector.of(s);
    }
}