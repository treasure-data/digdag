package io.digdag.server.metrics;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.server.metrics.jmx.JmxPluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DigdagMetricsConfig
{
    Logger logger = LoggerFactory.getLogger(DigdagMetricsConfig.class);
    Map<String, DigdagMetricsPluginConfig> plugins = new HashMap<>();

    public Optional<DigdagMetricsPluginConfig> getPluginConfig(String key)
    {
        return plugins.containsKey(key) ? Optional.of(plugins.get(key)) : Optional.absent();
    }

    public DigdagMetricsConfig(Config config)
    {
        // fetch enabled plugin server.metrics.enable = jmx,...
        List<String> keys = config
                .getOptional("server.metrics.enable", String.class)
                .transform((s) ->
                        Arrays.asList(s.split(","))
                                .stream()
                                .map((x) -> x.trim())
                                .distinct()
                                .collect(Collectors.toList())
                ).or(Arrays.asList());
        // load each config
        keys.stream().forEach((k) -> plugins.put(k, loadPluginConfig(k, config)));
    }

    /**
     * Override for extend
     * @param key
     * @param config
     * @return
     */
    public DigdagMetricsPluginConfig loadPluginConfig(String key, Config config)
    {
        switch (key) {
            case "jmx":
                return getJmxPluginConfig(config);
            default:
                throw new ConfigException("Unsupported digdag-metrics plugin:" + key);
        }
    }

    JmxPluginConfig getJmxPluginConfig(Config config)
    {
        return JmxPluginConfig.load(config);
    }
}
