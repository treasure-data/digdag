package io.digdag.core.agent;

import com.google.common.base.Optional;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.SecretNotFoundException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static io.digdag.client.config.ConfigUtils.newConfig;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;

public class GrantedPrivilegedVariablesTest
{
    @Test
    public void testExistance()
    {
        Config grants = newConfig()
            .set("params_exists", "param1")
            .set("params_not_exists", "param2")
            .set("shared_secret_exists", "shared1")
            .set("shared_secret_not_exists", "shared2")
            .set("only_secret_exists", newConfig().set("secret", "only1"))
            .set("only_secret_not_exists", newConfig().set("secret", "only2"));

        Config params = newConfig()
            .set("param1", "param1-value");

        Config secrets = newConfig()
            .set("shared1", "shared1-value")
            .set("only1", "only1-value");

        PrivilegedVariables vars = GrantedPrivilegedVariables.build(
                grants, params, (key) -> secrets.getOptional(key, String.class));

        assertThat(vars.getKeys(),
                is(asList("params_exists", "params_not_exists", "shared_secret_exists", "shared_secret_not_exists", "only_secret_exists", "only_secret_not_exists")));

        assertThat(vars.getOptional("params_exists"), is(Optional.of("param1-value")));
        assertThat(vars.getOptional("params_not_exists"), is(Optional.absent()));
        assertThat(vars.getOptional("shared_secret_exists"), is(Optional.of("shared1-value")));
        assertThat(vars.getOptional("shared_secret_not_exists"), is(Optional.absent()));
        assertThat(vars.getOptional("only_secret_exists"), is(Optional.of("only1-value")));
        assertThat(vars.getOptional("only_secret_not_exists"), is(Optional.absent()));
        assertThat(vars.getOptional("no_such_key"), is(Optional.absent()));

        assertThat(vars.get("params_exists"), is("param1-value"));
        assertException(() -> vars.get("params_not_exists"), ConfigException.class, "param2");
        assertThat(vars.get("shared_secret_exists"), is("shared1-value"));
        assertException(() -> vars.get("shared_secret_not_exists"), ConfigException.class, "shared2");
        assertThat(vars.get("only_secret_exists"), is("only1-value"));
        assertException(() -> vars.get("only_secret_not_exists"), SecretNotFoundException.class, "only2");
        assertNull(vars.get("no_such_key"));
    }

    @Test
    public void testNested()
    {
        Config grants = newConfig()
            .set("exists", "nest.param1")
            .set("key_not_exists", "nest.param2")
            .set("nest_not_exists", "no.param3")
            .set("secret_shared_exists", "nest.shared1")
            .set("secret_only_exists", "nest.only1");

        Config params = newConfig()
            .set("nest", newConfig().set("param1", "param1-value"));

        Config secrets = newConfig()
            .set("nest.shared1", "shared1-value")
            .set("nest.only1", "only1-value");

        PrivilegedVariables vars = GrantedPrivilegedVariables.build(
                grants, params, (key) -> secrets.getOptional(key, String.class));

        assertThat(vars.getOptional("exists"), is(Optional.of("param1-value")));
        assertThat(vars.getOptional("key_not_exists"), is(Optional.absent()));
        assertThat(vars.getOptional("nest_not_exists"), is(Optional.absent()));
        assertThat(vars.getOptional("secret_shared_exists"), is(Optional.of("shared1-value")));
        assertThat(vars.getOptional("secret_only_exists"), is(Optional.of("only1-value")));

        assertThat(vars.get("exists"), is("param1-value"));
        assertException(() -> vars.get("key_not_exists"), ConfigException.class, "nest.param2");
        assertException(() -> vars.get("nest_not_exists"), ConfigException.class, "no.param3");
        assertThat(vars.get("secret_shared_exists"), is("shared1-value"));
        assertThat(vars.get("secret_only_exists"), is("only1-value"));
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
