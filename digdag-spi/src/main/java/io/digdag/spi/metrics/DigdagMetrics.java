package io.digdag.spi.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface DigdagMetrics
{
    Logger logger = LoggerFactory.getLogger(DigdagMetrics.class);

    enum Category {
        DEFAULT("default"),
        AGENT("agent"),
        API("api"),
        DB("db"),
        EXECUTOR("executor");

        private final String text;
        Category(final String text) { this.text = text; }

        public String getString() { return this.text; }

        public static Category fromString(String text)
        {
            for (Category c : Category.values()) {
                if (c.text.compareToIgnoreCase(text) == 0) {
                    return c;
                }
            }
            logger.error("Invalid category name {}. Fallback to DEFAULT", text);
            return DEFAULT;
        }
    }

    MeterRegistry getRegistry();

    MeterRegistry getRegistry(Category category);

    String mkMetricsName(Category category, String metricsName);

    void increment(Category category, String metricName, Tags tags);

    void gauge(Category category, String metricName, Tags tags, double value);

    void summary(Category category, String metricName, Tags tags, double value);

    default void increment(String metricName)
    {
        increment(Category.DEFAULT, metricName);
    }

    default void increment(String metricName, Tags tags)
    {
        increment(Category.DEFAULT, metricName, tags);
    }

    default void increment(Category category, String metricName)
    {
        increment(category, metricName, Tags.empty());
    }

    default void gauge(String metricName, double value)
    {
        gauge(Category.DEFAULT, metricName, Tags.empty(), value);
    }

    default void gauge(String metricName, Tags tags, double value)
    {
        gauge(Category.DEFAULT, metricName, tags, value);
    }

    default void summary(String metricName, double value)
    {
        summary(Category.DEFAULT, metricName, value);
    }

    default void summary(Category category, String metricName, double value)
    {
        summary(category, metricName, Tags.empty(), value);
    }

    default Timer.Sample timerStart(Category category)
    {
        return Timer.start(getRegistry(category));
    }

    default void timerStop(Category category, String metricsName, Tags tags, Timer.Sample sample)
    {
        Timer.Builder builder = Timer.builder(metricsName)
                .tags(tags)
                .publishPercentileHistogram(true)
                ;
        sample.stop(builder.register(getRegistry(category)));
    }

}
