package io.digdag.util;

import com.google.common.base.Optional;
import io.digdag.client.config.ConfigException;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class UserSecretTemplateTest
{
    @Test
    public void testCorrectTemplates()
            throws Exception
    {
        assertThat(UserSecretTemplate.of("${}").format(key -> Optional.of("bar")), is("${}"));
        assertThat(UserSecretTemplate.of("${foo}").format(key -> Optional.of("bar")), is("${foo}"));
        assertThat(UserSecretTemplate.of("${secret:foo}").format(key -> Optional.of("bar")), is("bar"));
        assertThat(UserSecretTemplate.of("${secret:foo}").format(key -> Optional.of("$b$a$r$")), is("$b$a$r$"));
        assertThat(UserSecretTemplate.of("${secret:foo}").format(key -> Optional.of("\\$b\\$a\\$r\\$")), is("\\$b\\$a\\$r\\$"));
        assertThat(UserSecretTemplate.of("hello ${secret:foo} world").format(key -> Optional.of("bar")), is("hello bar world"));
        assertThat(UserSecretTemplate.of("hello ${secret:foo} world ${secret:bar}").format(key -> {
            switch (key) {
                case "foo":
                    return Optional.of("fooval");
                case "bar":
                    return Optional.of("barval");
                default:
                    throw new AssertionError();
            }
        }), is("hello fooval world barval"));
    }

    @Test
    public void testInvalidTemplates()
            throws Exception
    {
        try {
            UserSecretTemplate.of("${secrets:foo}");
            fail();
        }
        catch (ConfigException e) {
            assertThat(e.getMessage(), is("Invalid parametrization: '${secrets:foo}'"));
        }

        try {
            UserSecretTemplate.of("${bar:foo}");
            fail();
        }
        catch (ConfigException e) {
            assertThat(e.getMessage(), is("Invalid parametrization: '${bar:foo}'"));
        }

        try {
            UserSecretTemplate.of("${:foo}");
            fail();
        }
        catch (ConfigException e) {
            assertThat(e.getMessage(), is("Invalid parametrization: '${:foo}'"));
        }
    }
}