package io.digdag.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AbstractWaitOperatorFactoryTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new GuavaModule());

    private static final ConfigFactory CONFIG_FACTORY = new ConfigFactory(MAPPER);

    @Test
    public void testConfigurationsValidation()
            throws Exception
    {
        // Check value bounds / parsing
        String[] keys = {
                "config.test.wait.poll_interval",
                "config.test.wait.min_poll_interval",
                "config.test.wait.max_poll_interval"
        };
        String[] illegalValues = {
                "",
                "foobar",
                "-1s",
                "99999999999999999s",
                (SECONDS.toHours(Integer.MAX_VALUE) * 2) + "h"
        };
        for (String key : keys) {
            for (String value : illegalValues) {
                assertConfigurationFailsValidation(ImmutableMap.of(key, value));
            }
        }

        // Check that the min/max bounds apply to the configured default poll interval
        assertConfigurationFailsValidation(ImmutableMap.of("config.test.wait.poll_interval", "5s"));
        assertConfigurationFailsValidation(ImmutableMap.of("config.test.wait.poll_interval", "29s"));
        assertConfigurationPassesValidation(ImmutableMap.of("config.test.wait.poll_interval", "30s"));
        assertConfigurationPassesValidation(ImmutableMap.of("config.test.wait.poll_interval", "31s"));


        // Check that the default poll interval is adjusted to fit min/max
        {
            AbstractWaitOperatorFactory f = assertConfigurationPassesValidation(ImmutableMap.of(
                    "config.test.wait.min_poll_interval", "45s",
                    "config.test.wait.max_poll_interval", "60s"
            ));
            assertThat(f.getPollInterval(CONFIG_FACTORY.create()), is(45));
        }

        // fail: poll < min
        assertConfigurationFailsValidation(ImmutableMap.of(
                "config.test.wait.min_poll_interval", "10s",
                "config.test.wait.poll_interval", "5s"
        ));

        // fail: poll > max
        assertConfigurationFailsValidation(ImmutableMap.of(
                "config.test.wait.min_poll_interval", "30s",
                "config.test.wait.max_poll_interval", "40s",
                "config.test.wait.poll_interval", "50s"
        ));

        // fail: max < min
        assertConfigurationFailsValidation(ImmutableMap.of(
                "config.test.wait.min_poll_interval", "10s",
                "config.test.wait.max_poll_interval", "5s"
        ));

        {
            AbstractWaitOperatorFactory f = assertConfigurationPassesValidation(ImmutableMap.of(
                    "config.test.wait.min_poll_interval", "5s",
                    "config.test.wait.poll_interval", "5s",
                    "config.test.wait.max_poll_interval", "5s"
            ));
            assertThat(f.getPollInterval(CONFIG_FACTORY.create()), is(5));
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("interval", "3s")))), ConfigException.class);
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("interval", "7s")))), ConfigException.class);
        }

        {
            AbstractWaitOperatorFactory f = assertConfigurationPassesValidation(ImmutableMap.of(
                    "config.test.wait.min_poll_interval", "5s",
                    "config.test.wait.poll_interval", "10s",
                    "config.test.wait.max_poll_interval", "15s"
            ));
            assertThat(f.getPollInterval(CONFIG_FACTORY.create()), is(10));
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("interval", "3s")))), ConfigException.class);
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("interval", "30s")))), ConfigException.class);
        }

        {
            AbstractWaitOperatorFactory f = assertConfigurationPassesValidation(ImmutableMap.of(
                    "config.test.wait.min_poll_interval", "10s",
                    "config.test.wait.poll_interval", "5m",
                    "config.test.wait.max_poll_interval", "2h"
            ));
            assertThat(f.getPollInterval(CONFIG_FACTORY.create()), is(5 * 60));
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("interval", "5s")))), ConfigException.class);
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("interval", "3h")))), ConfigException.class);
        }
    }

    private void assertConfigurationFailsValidation(Map<String, Object> config)
            throws JsonProcessingException
    {
        assertThrows(() -> new AbstractWaitOperatorFactory("test.wait", cfg(config)) {}, ConfigException.class);
    }

    private Config cfg(Map<String, Object> config)
            throws JsonProcessingException
    {
        String json = MAPPER.writeValueAsString(config);
        return CONFIG_FACTORY.fromJsonString(json);
    }

    private AbstractWaitOperatorFactory assertConfigurationPassesValidation(Map<String, Object> config)
            throws JsonProcessingException
    {
        Config c = cfg(config);
        return new AbstractWaitOperatorFactory("test.wait", c) {};
    }

    static <T, E extends Throwable> void assertThrows(Callable<T> callable, Class<E> exceptionClass)
    {
        try {
            callable.call();
        }
        catch (Throwable e) {
            assertThat(e, Matchers.instanceOf(exceptionClass));
            return;
        }
        Assert.fail("Expected an exception of type: " + exceptionClass);
    }
}
