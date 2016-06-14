package io.digdag.standards.operator.td;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import org.junit.Test;

import java.util.Map;

import static io.digdag.standards.operator.td.Utils.assertThrows;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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
                "config.td.wait.poll_interval",
                "config.td.wait.min_poll_interval",
                "config.td.wait.max_poll_interval"
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
        assertConfigurationFailsValidation(ImmutableMap.of("config.td.wait.poll_interval", "5s"));
        assertConfigurationFailsValidation(ImmutableMap.of("config.td.wait.poll_interval", "29s"));
        assertConfigurationPassesValidation(ImmutableMap.of("config.td.wait.poll_interval", "30s"));
        assertConfigurationPassesValidation(ImmutableMap.of("config.td.wait.poll_interval", "31s"));


        // Check that the default poll interval is adjusted to fit min/max
        {
            AbstractWaitOperatorFactory f = assertConfigurationPassesValidation(ImmutableMap.of(
                    "config.td.wait.min_poll_interval", "45s",
                    "config.td.wait.max_poll_interval", "60s"
            ));
            assertThat(f.getPollInterval(CONFIG_FACTORY.create()), is(45));
        }

        // fail: poll < min
        assertConfigurationFailsValidation(ImmutableMap.of(
                "config.td.wait.min_poll_interval", "10s",
                "config.td.wait.poll_interval", "5s"
        ));

        // fail: poll > max
        assertConfigurationFailsValidation(ImmutableMap.of(
                "config.td.wait.min_poll_interval", "30s",
                "config.td.wait.max_poll_interval", "40s",
                "config.td.wait.poll_interval", "50s"
        ));

        // fail: max < min
        assertConfigurationFailsValidation(ImmutableMap.of(
                "config.td.wait.min_poll_interval", "10s",
                "config.td.wait.max_poll_interval", "5s"
        ));

        {
            AbstractWaitOperatorFactory f = assertConfigurationPassesValidation(ImmutableMap.of(
                    "config.td.wait.min_poll_interval", "5s",
                    "config.td.wait.poll_interval", "5s",
                    "config.td.wait.max_poll_interval", "5s"
            ));
            assertThat(f.getPollInterval(CONFIG_FACTORY.create()), is(5));
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("poll", "3s")))), ConfigException.class);
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("poll", "7s")))), ConfigException.class);
        }

        {
            AbstractWaitOperatorFactory f = assertConfigurationPassesValidation(ImmutableMap.of(
                    "config.td.wait.min_poll_interval", "5s",
                    "config.td.wait.poll_interval", "10s",
                    "config.td.wait.max_poll_interval", "15s"
            ));
            assertThat(f.getPollInterval(CONFIG_FACTORY.create()), is(10));
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("poll", "3s")))), ConfigException.class);
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("poll", "30s")))), ConfigException.class);
        }

        {
            AbstractWaitOperatorFactory f = assertConfigurationPassesValidation(ImmutableMap.of(
                    "config.td.wait.min_poll_interval", "10s",
                    "config.td.wait.poll_interval", "5m",
                    "config.td.wait.max_poll_interval", "2h"
            ));
            assertThat(f.getPollInterval(CONFIG_FACTORY.create()), is(5 * 60));
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("poll", "5s")))), ConfigException.class);
            assertThrows(() -> f.getPollInterval(CONFIG_FACTORY.create(cfg(ImmutableMap.of("poll", "3h")))), ConfigException.class);
        }
    }

    private void assertConfigurationFailsValidation(Map<String, Object> config)
            throws JsonProcessingException
    {
        assertThrows(() -> new AbstractWaitOperatorFactory(cfg(config)) {}, ConfigException.class);
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
        return new AbstractWaitOperatorFactory(c) {};
    }
}