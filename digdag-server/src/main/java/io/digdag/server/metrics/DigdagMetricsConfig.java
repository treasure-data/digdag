package io.digdag.server.metrics;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.server.metrics.jmx.JmxPluginConfig;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Value.Immutable
@JsonDeserialize(as = ImmutableDigdagMetricsConfig.class)
public interface DigdagMetricsConfig
{
    Logger logger = LoggerFactory.getLogger(DigdagMetricsConfig.class);

    enum Plugin{ JMX }

    List<Plugin> ALL = Arrays.asList(Plugin.class.getEnumConstants());

    @Value.Default
    default JmxPluginConfig getJmxPluginConfig()
    {
        return JmxPluginConfig.disabled();
    }

    default DigdagMetricsPluginConfig getPluginConfig(Plugin plugin)
    {
        switch (plugin) {
            case JMX:
                return getJmxPluginConfig();
            default:
                throw new ConfigException("Unsupported digdag-metrics plugin:" + plugin.name());
        }
    }


    static ImmutableDigdagMetricsConfig.Builder loadPluginConfig(ImmutableDigdagMetricsConfig.Builder builder, Plugin plugin, Config config)
    {
        switch (plugin) {
            case JMX:
                return builder.jmxPluginConfig(JmxPluginConfig.load(config));
            default:
                throw new ConfigException("Unsupported digdag-metrics plugin:" + plugin.name());
        }
    }

    static Plugin getPluginEnum(String plugin)
    {
        switch (plugin) {
            case "jmx":
                return Plugin.JMX;
            default:
                throw new ConfigException("Unknown metrics plugin:" + plugin);
        }
    }

    /**
     *
     * @param config
     *   server.metrics.enabled: jmx
     *   server.metrics.jmx.categories: agent,executor
     * @return
     */
    static DigdagMetricsConfig load(Config config)
    {
        Optional<List<Plugin>> enables = config
                .getOptional("server.metrics.enable", String.class)
                .transform( (s) ->
                    Arrays.asList(s.split(","))
                        .stream()
                        .map( (x) -> x.trim())
                        .map( (x) -> getPluginEnum(x))
                        .distinct()
                        .collect(Collectors.toList())
                )
                ;
        ImmutableDigdagMetricsConfig.Builder builder = ImmutableDigdagMetricsConfig.builder();
        if (enables.isPresent()) {
            enables.get().stream().forEach( (x) -> loadPluginConfig(builder, x, config));
        }
        return builder.build();
    }
}
