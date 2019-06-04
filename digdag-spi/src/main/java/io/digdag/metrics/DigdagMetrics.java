package io.digdag.metrics;

import com.google.inject.Inject;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;


public class DigdagMetrics
{
    @Inject
    MeterRegistry registry;

    @Inject
    public DigdagMetrics()
    {
    }

    public MeterRegistry getRegistry()
    {
        return registry;
    }

    public void increment(String metricName, Tags tags)
    {
        registry.counter(metricName, tags).increment();
    }

    public void increment(String metricName)
    {
        registry.counter(metricName).increment();
    }

    public void gauge(String metricName, double value)
    {
        registry.gauge(metricName, value);
    }

    public void gauge(String metricName, long value)
    {
        registry.gauge(metricName, value);
    }

    public static DigdagMetrics empty()
    {
        DigdagMetrics metrics = new DigdagMetrics();
        metrics.registry = new SimpleMeterRegistry();
        return metrics;
    }
}
