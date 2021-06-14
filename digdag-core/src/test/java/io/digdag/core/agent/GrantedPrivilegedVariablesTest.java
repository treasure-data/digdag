package io.digdag.core.agent;

import com.google.common.base.Optional;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretNotFoundException;

import org.junit.Test;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class GrantedPrivilegedVariablesTest
{
    @Test
    public void testExistance()
    {
        Config grants = newConfig()
            .set("simple", "${secret:secret1}")
            .set("concat", "${secret:secret1}-${secret:secret1}")
            .set("no_secret", "${secret:no_such_secret}")
            ;

        Config secrets = newConfig()
            .set("secret1", "secv")
            ;

        PrivilegedVariables vars = GrantedPrivilegedVariables.build(
                grants, (key) -> secrets.getOptional(key, String.class));

        assertThat(vars.getKeys(),
                is(asList("simple", "concat", "no_secret")));

        assertThat(vars.get("simple"), is("secv"));
        assertThat(vars.get("concat"), is("secv-secv"));
        assertThat(vars.getOptional("simple"), is(Optional.of("secv")));
        assertThat(vars.getOptional("concat"), is(Optional.of("secv-secv")));

        assertException(() -> vars.get("no_secret"), SecretNotFoundException.class, "no_such_secret");
        assertException(() -> vars.getOptional("no_secret"), SecretNotFoundException.class, "no_such_secret");

        assertException(() -> vars.get("not_exists"), ConfigException.class, "not_exists");
        assertThat(vars.getOptional("not_exists"), is(Optional.absent()));
    }

    @Test
    public void testNested()
    {
        Config grants = newConfig()
            .set("simple", "${secret:nested.secret1}")
            .set("concat", "${secret:nested.secret1}-${secret:nested.secret1}")
            .set("no_secret", "${secret:nested.no_such_secret}")
            ;

        Config secrets = newConfig()
            .set("nested.secret1", "secv")
            ;

        PrivilegedVariables vars = GrantedPrivilegedVariables.build(
                grants, (key) -> secrets.getOptional(key, String.class));

        assertThat(vars.getKeys(),
                is(asList("simple", "concat", "no_secret")));

        assertThat(vars.get("simple"), is("secv"));
        assertThat(vars.get("concat"), is("secv-secv"));
        assertThat(vars.getOptional("simple"), is(Optional.of("secv")));
        assertThat(vars.getOptional("concat"), is(Optional.of("secv-secv")));

        assertException(() -> vars.get("no_secret"), SecretNotFoundException.class, "nested.no_such_secret");
        assertException(() -> vars.getOptional("no_secret"), SecretNotFoundException.class, "nested.no_such_secret");
    }

    private void assertException(Runnable func, Class<? extends Exception> expected, String message)
    {
        try {
            func.run();
            fail();
        }
        catch (Exception ex) {
            assertThat(ex, instanceOf(expected));
            assertThat(ex.getMessage(), containsString(message));
        }
    }
}
