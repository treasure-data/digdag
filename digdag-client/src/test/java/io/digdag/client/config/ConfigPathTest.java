package io.digdag.client.config;

import org.junit.Test;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.is;

public class ConfigPathTest
{
    @Test
    public void verifyParseAndToString()
    {
        assertReversible("/a", 1);
        assertReversible("/foo/bar", 2);
        assertReversible("/foo/bar/baz", 3);
    }

    @Test
    public void rejectInvalids()
    {
        assertInvalid("");
        assertInvalid("a/");
        assertInvalid("a");
        assertInvalid("/");
        assertInvalid("/a/");
        assertInvalid("//a");
        assertInvalid("/a//");
        assertInvalid("/a//b");
        assertInvalid("//x");
        assertInvalid("/x/\"abc\"/x");
    }

    private static void assertReversible(String key, int count)
    {
        assertThat(ConfigPath.parse(key).toString(), is(key));
        assertThat(ConfigPath.parse(key).getNames().size(), is(count));
    }

    private static void assertInvalid(String key)
    {
        try {
            ConfigPath.parse(key);
            fail();
        }
        catch (IllegalArgumentException ex) {
            assertTrue(true);
        }
    }
}
