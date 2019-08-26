package io.digdag.server.metrics.jmx;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.server.metrics.MonitorSystemConfig;
import io.digdag.spi.metrics.DigdagMetrics.Category;
import org.immutables.value.Value;

import java.util.Map;

@Value.Immutable
@JsonDeserialize(as = ImmutableJmxMonitorSystemConfig.class)
public interface JmxMonitorSystemConfig extends MonitorSystemConfig
{
    @Value.Default
    default boolean getMonitorSystemEnable() { return true; }

    @Value.Default
    default boolean getCategoryDefaultEnable() { return true; }

    @Value.Default
    default boolean getCategoryAgentEnable() { return true; }

    @Value.Default
    default boolean getCategoryApiEnable() { return true; }

    @Value.Default
    default boolean getCategoryDbEnable() { return true; }

    @Value.Default
    default boolean getCategoryExecutorEnable() { return true; }

    /**
     *
     * @param config
     *   jmx.categories: ALL
     * @return
     */
    static JmxMonitorSystemConfig load(Config config)
    {
        Map<Category, Boolean> categories = MonitorSystemConfig.getEnabledCategories(config.getOptional("metrics.jmx.categories", String.class));
        return ImmutableJmxMonitorSystemConfig
                .builder()
                .categoryAgentEnable(categories.get(Category.AGENT))
                .categoryApiEnable(categories.get(Category.API))
                .categoryDbEnable(categories.get(Category.DB))
                .categoryExecutorEnable(categories.get(Category.EXECUTOR))
                .categoryDefaultEnable(categories.get(Category.DEFAULT))
                .build();
    }

    static JmxMonitorSystemConfig disabled()
    {
        return ImmutableJmxMonitorSystemConfig.builder().monitorSystemEnable(false).build();
    }
}
