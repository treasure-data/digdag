package io.digdag.core.agent;

import io.digdag.spi.SecretSelector;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SecretSelectorTest
{
    @Test
    public void match()
            throws Exception
    {
        assertThat(SecretSelector.of("foo").match("foo"), is(true));
        assertThat(SecretSelector.of("foo").match("bar"), is(false));
        assertThat(SecretSelector.of("foo.*").match("foo.bar"), is(true));
        assertThat(SecretSelector.of("foo.*").match("foo"), is(false));
        assertThat(SecretSelector.of("foo.*").match("bar"), is(false));
    }
}