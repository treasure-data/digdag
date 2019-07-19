package io.digdag.spi.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public interface DigdagMetrics
{
    MeterRegistry getRegistry();

    MeterRegistry getRegistry(String category);

    String mkMetricsName(String category, String metricsName);

    void increment(String category, String metricName, Tags tags);

    void gauge(String category, String metricName, Tags tags, double value);

    void summary(String category, String metricName, Tags tags, double value);

    default void increment(String metricName)
    {
        increment("default", metricName);
    }

    default void increment(String metricName, Tags tags)
    {
        increment("default", metricName, tags);
    }

    default void increment(String category, String metricName)
    {
        increment(category, metricName, Tags.empty());
    }

    default void gauge(String metricName, double value)
    {
        gauge("default", metricName, Tags.empty(), value);
    }

    default void gauge(String metricName, Tags tags, double value)
    {
        gauge("default", metricName, tags, value);
    }

    default void summary(String metricName, double value)
    {
        summary("default", metricName, value);
    }

    default void summary(String category, String metricName, double value)
    {
        summary(category, metricName, Tags.empty(), value);
    }

    default Timer.Sample timerStart(String category)
    {
        return Timer.start(getRegistry(category));
    }

    default void timerStop(String category, String metricsName, Tags tags, Timer.Sample sample)
    {
        Timer.Builder builder = Timer.builder(metricsName)
                .tags(tags)
                .publishPercentileHistogram(true)
                ;
        sample.stop(builder.register(getRegistry(category)));
    }

}
