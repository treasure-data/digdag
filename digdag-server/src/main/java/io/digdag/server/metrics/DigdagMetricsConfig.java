package io.digdag.server.metrics;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.server.metrics.jmx.JmxMonitorSystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DigdagMetricsConfig
{
    private static Logger logger = LoggerFactory.getLogger(DigdagMetricsConfig.class);
    private Map<String, MonitorSystemConfig> monitorSystems = new HashMap<>();

    public Optional<MonitorSystemConfig> getMonitorSystemConfig(String key)
    {
        return monitorSystems.containsKey(key) ? Optional.of(monitorSystems.get(key)) : Optional.absent();
    }

    public DigdagMetricsConfig(Config config)
    {
        // fetch enabled monitor system metrics.enable = jmx,...
        List<String> keys = config
                .getOptional("metrics.enable", String.class)
                .transform((s) ->
                        Arrays.asList(s.split(","))
                                .stream()
                                .map((x) -> x.trim())
                                .distinct()
                                .collect(Collectors.toList())
                ).or(Arrays.asList());
        // load each config
        keys.stream().forEach((k) -> monitorSystems.put(k, loadMonitorSystemConfig(k, config)));
    }

    /**
     * Override for extend
     * @param key
     * @param config
     * @return
     */
    public MonitorSystemConfig loadMonitorSystemConfig(String key, Config config)
    {
        switch (key) {
            case "jmx":
                return getJmxMonitorSystemConfig(config);
            default:
                throw new ConfigException("Unsupported digdag-metrics monitor system:" + key);
        }
    }

    JmxMonitorSystemConfig getJmxMonitorSystemConfig(Config config)
    {
        return JmxMonitorSystemConfig.load(config);
    }
}
