package io.digdag.metrics;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.digdag.spi.metrics.DigdagMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 *  JMXMeterRegistry cannot change the domain. So hold multiple MeterRegistry.
 */
public class StdDigdagMetrics implements DigdagMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(StdDigdagMetrics.class);

    final MeterRegistry registry;
    final MeterRegistry registryAgent;
    final MeterRegistry registryApi;
    final MeterRegistry registryDb;
    final MeterRegistry registryExecutor;
    final Map<Category, MeterRegistry> mapRegistries;

    @Inject
    public StdDigdagMetrics(
            @Named("default") MeterRegistry registry, @Named("agent") MeterRegistry registryAgent,
            @Named("api") MeterRegistry registryApi, @Named("db") MeterRegistry registryDb,
            @Named("executor") MeterRegistry registryExecutor
    )
    {
        this.registry = registry;
        this.registryAgent = registryAgent;
        this.registryApi = registryApi;
        this.registryDb = registryDb;
        this.registryExecutor = registryExecutor;
        this.mapRegistries = ImmutableMap.of(
                Category.DEFAULT, registry,
                Category.AGENT, registryAgent,
                Category.API, registryApi,
                Category.DB, registryDb,
                Category.EXECUTOR, registryExecutor);
    }

    @Override
    public MeterRegistry getRegistry()
    {
        return registry;
    }

    @Override
    public MeterRegistry getRegistry(Category category)
    {
        MeterRegistry mreg =  mapRegistries.get(category);
        if (mreg == null) {
            logger.warn("Cannot get MeterRegistry for {}", category);
            mreg = registry;
        }
        return mreg;
    }

    @Override
    public String mkMetricsName(Category category, String metricsName)
    {
        return metricsPrefix(category) + metricsName;
    }

    private String metricsPrefix(Category category)
    {
        if (category.getString().compareTo("default") == 0) {
            return "";
        }
        else {
            return category.getString() + "_";
        }
    }

    @Override
    public void increment(Category category, String metricName, Tags tags)
    {
        getRegistry(category).counter(mkMetricsName(category, metricName), tags).increment();
    }

    @Override
    public void gauge(Category category, String metricName, Tags tags, double value)
    {
        getRegistry(category).gauge(mkMetricsName(category, metricName), tags, value);
    }

    @Override
    public void summary(Category category, String metricName, Tags tags, double value)
    {
        getRegistry(category).summary(mkMetricsName(category, metricName), tags).record(value);
    }

    public static StdDigdagMetrics empty()
    {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        StdDigdagMetrics metrics = new StdDigdagMetrics(registry, registry, registry, registry, registry);
        return metrics;
    }
}
