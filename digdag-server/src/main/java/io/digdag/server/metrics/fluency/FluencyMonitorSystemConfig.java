package io.digdag.server.metrics.fluency;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.server.metrics.MonitorSystemConfig;
import io.digdag.spi.metrics.DigdagMetrics.Category;
import org.immutables.value.Value;
import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.fluentd.FluencyBuilderForFluentd;

import java.util.Map;

@Value.Immutable
@JsonDeserialize(as = ImmutableFluencyMonitorSystemConfig.class)
public interface FluencyMonitorSystemConfig extends MonitorSystemConfig
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

    @Value.Default
    default String getType() { return "fluentd"; }

    @Value.Default
    default String getTag() { return "digdag"; }

    @Value.Default
    default String getHost() { return "localhost:24224"; }

    @Value.Default
    default long getStep() { return 60L; }

    @Value.Default
    default Optional<String> getBackupDir() { return Optional.absent(); }


    /**
     *
     * @param config
     *   server.metrics.fluency.categories: agent,executor,...
     * @return
     */
    static FluencyMonitorSystemConfig load(Config config)
    {
        Map<Category, Boolean> categories = MonitorSystemConfig.getEnabledCategories(config.getOptional("metrics.fluency.categories", String.class));
        return ImmutableFluencyMonitorSystemConfig
                .builder()
                .monitorSystemEnable(true)
                .categoryAgentEnable(categories.get(Category.AGENT))
                .categoryApiEnable(categories.get(Category.API))
                .categoryDbEnable(categories.get(Category.DB))
                .categoryExecutorEnable(categories.get(Category.EXECUTOR))
                .categoryDefaultEnable(categories.get(Category.DEFAULT))
                .type(config.getOptional("metrics.fluency.type", String.class).or("fluentd"))
                .tag(config.getOptional("metrics.fluency.tag", String.class).or("digdag"))
                .host(config.getOptional("metrics.fluency.host", String.class).or("localhost:24224"))
                .backupDir(config.getOptional("metrics.fluency.host", String.class))
                .step(config.getOptional("metrics.fluency.step", Long.class).or(60L))
                .build();
    }

    static Fluency createFluency(FluencyMonitorSystemConfig config)
    {
        try {
            if (!config.getType().equalsIgnoreCase("fluentd")) {
                throw new ConfigException("No supported type in FluencyMeterRegistry:" + config.getType());
            }
            String[] hostAndPort = config.getHost().split(":", 2);
            int port = hostAndPort.length == 1 ? 2424: Integer.parseInt(hostAndPort[1]);
            return new FluencyBuilderForFluentd().build(hostAndPort[0], port);
        }
        catch (ConfigException ce) {
            throw ce;
        }
        catch (Exception e) {
            throw new ConfigException("Invalid configuration in FluencyMeterRegistry:" + e.toString());
        }
    }


    static FluencyMonitorSystemConfig disabled()
    {
        return ImmutableFluencyMonitorSystemConfig.builder().monitorSystemEnable(false).build();
    }
}
