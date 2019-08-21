package io.digdag.server.metrics;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigFactory;
import io.digdag.server.metrics.fluency.FluencyMonitorSystemConfig;
import io.digdag.server.metrics.fluency.ImmutableFluencyMonitorSystemConfig;
import io.digdag.server.metrics.jmx.ImmutableJmxMonitorSystemConfig;
import io.digdag.server.metrics.jmx.JmxMonitorSystemConfig;
import io.digdag.spi.metrics.DigdagMetrics;
import static io.digdag.client.DigdagClient.objectMapper;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

public class DigdagMetricsConfigTest
{
    Logger logger = LoggerFactory.getLogger(DigdagMetricsConfigTest.class);

    Config config;

    @Before
    public void setup()
    {
        config = ConfigElement.ofMap(new HashMap<String,String>()).toConfig(new ConfigFactory(objectMapper()));
    }

    public Config fromJson(String json)
    {
        return ConfigElement.fromJson(json).toConfig(new ConfigFactory(objectMapper()));
    }

    @Test
    public void testDefault()
    {
        DigdagMetricsConfig metricsConfig = new DigdagMetricsConfig(config);
        assertFalse("No jmx config", metricsConfig.getMonitorSystemConfig("jmx").isPresent());
    }

    @Test
    public void testJmxEnabled()
    {
        /**
         *  metrics.enable: " jmx "
         */
        config = fromJson("{ \"metrics.enable\": \" jmx \" }");
        DigdagMetricsConfig metricsConfig = new DigdagMetricsConfig(config);

        Optional<JmxMonitorSystemConfig> jmxConfig = metricsConfig.getMonitorSystemConfig("jmx").transform((p) -> (JmxMonitorSystemConfig)p);
        assertTrue("Exist jmx config", jmxConfig.isPresent());
        assertTrue("plugin is enable", jmxConfig.get().getMonitorSystemEnable());
        assertTrue("category 'agent' is enable", jmxConfig.get().enable(DigdagMetrics.Category.AGENT));
        assertTrue("category 'api' is enable", jmxConfig.get().enable(DigdagMetrics.Category.API));
        assertTrue("category 'db' is enable", jmxConfig.get().enable(DigdagMetrics.Category.DB));
        assertTrue("category 'executor' is enable", jmxConfig.get().enable(DigdagMetrics.Category.EXECUTOR));
        assertTrue("category 'default' is enable", jmxConfig.get().enable(DigdagMetrics.Category.DEFAULT));
    }

    @Test
    public void testJmxEnabledWithParams()
    {
        /**
         *  server.metrics.enable: " jmx "
         *  server.metrics.jmx.categories: "agent, executors "
         */
        config = fromJson(
                "{ " +
                        "\"metrics.enable\": \" jmx \", " +
                        "\"metrics.jmx.categories\": \"agent, executor\" " +
                 "}");
        DigdagMetricsConfig metricsConfig = new DigdagMetricsConfig(config);

        Optional<JmxMonitorSystemConfig> jmxConfig = metricsConfig.getMonitorSystemConfig("jmx").transform((p) -> (JmxMonitorSystemConfig)p);
        assertTrue("Exist jmx config", jmxConfig.isPresent());
        assertTrue("plugin is enable", jmxConfig.get().getMonitorSystemEnable());
        assertTrue("category 'agent' is enable", jmxConfig.get().enable(DigdagMetrics.Category.AGENT));
        assertFalse("category 'api' is enable", jmxConfig.get().enable(DigdagMetrics.Category.API));
        assertFalse("category 'db' is enable", jmxConfig.get().enable(DigdagMetrics.Category.DB));
        assertTrue("category 'executor' is enable", jmxConfig.get().enable(DigdagMetrics.Category.EXECUTOR));
        assertFalse("category 'default' is enable", jmxConfig.get().enable(DigdagMetrics.Category.DEFAULT));
    }


    @Test
    public void testFluencyEnabled()
    {
        /**
         *  metrics.enable: " fluency "
         */
        config = fromJson("{ \"metrics.enable\": \" fluency \" }");
        DigdagMetricsConfig metricsConfig = new DigdagMetricsConfig(config);

        Optional<FluencyMonitorSystemConfig> fluencyConfig = metricsConfig.getMonitorSystemConfig("fluency").transform((p) -> (FluencyMonitorSystemConfig)p);
        assertTrue("Exist fluency config", fluencyConfig.isPresent());
        assertTrue("plugin is enable", fluencyConfig.get().getMonitorSystemEnable());
        assertTrue("category 'agent' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.AGENT));
        assertTrue("category 'api' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.API));
        assertTrue("category 'db' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.DB));
        assertTrue("category 'executor' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.EXECUTOR));
        assertTrue("category 'default' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.DEFAULT));
    }

    @Test
    public void testFluencyEnabledWithParams()
    {
        /**
         *  server.metrics.enable: " fluency "
         *  server.metrics.jmx.categories: "agent, executors "
         */
        config = fromJson(
                "{ " +
                        "\"metrics.enable\": \" fluency \", " +
                        "\"metrics.fluency.categories\": \"agent, executor\", " +
                        "\"metrics.fluency.host\": \"server01:9999\", " +
                        "\"metrics.fluency.tag\": \"tag0001\" " +
                        "}");
        DigdagMetricsConfig metricsConfig = new DigdagMetricsConfig(config);

        Optional<FluencyMonitorSystemConfig> fluencyConfig = metricsConfig.getMonitorSystemConfig("fluency").transform((p) -> (FluencyMonitorSystemConfig)p);
        assertTrue("Exist fluency config", fluencyConfig.isPresent());
        assertTrue("plugin is enable", fluencyConfig.get().getMonitorSystemEnable());
        assertTrue("category 'agent' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.AGENT));
        assertFalse("category 'api' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.API));
        assertFalse("category 'db' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.DB));
        assertTrue("category 'executor' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.EXECUTOR));
        assertFalse("category 'default' is enable", fluencyConfig.get().enable(DigdagMetrics.Category.DEFAULT));
        assertEquals("host", "server01:9999", fluencyConfig.get().getHost());
        assertEquals("tag", "tag0001", fluencyConfig.get().getTag());
    }
}
