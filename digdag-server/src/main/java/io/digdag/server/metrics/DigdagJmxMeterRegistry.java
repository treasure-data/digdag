package io.digdag.server.metrics;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.core.lang.Nullable;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.ToDoubleFunction;

/**
 * Ignore tag because tag is appended to the MBean name and it is very noisy.
 */
public class DigdagJmxMeterRegistry extends JmxMeterRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(DigdagJmxMeterRegistry.class);

    public DigdagJmxMeterRegistry(JmxConfig config, Clock clock)
    {
        super(config, clock, HierarchicalNameMapper.DEFAULT);
    }

    @Override
    protected Counter newCounter(Meter.Id id) {
        return super.newCounter(removeTags(id));
    }

    @Override
    protected <T> Gauge newGauge(Meter.Id id, @Nullable T obj, ToDoubleFunction<T> valueFunction) {
        return super.newGauge(removeTags(id), obj, valueFunction);
    }

    @Override
    protected Timer newTimer(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, PauseDetector pauseDetector)
    {
        return super.newTimer(removeTags(id), distributionStatisticConfig, pauseDetector);
    }

    @Override
    protected DistributionSummary newDistributionSummary(Meter.Id id, DistributionStatisticConfig distributionStatisticConfig, double scale)
    {
        return super.newDistributionSummary(removeTags(id), distributionStatisticConfig, scale);
    }

    protected Meter.Id removeTags(Meter.Id id)
    {
        Meter.Id newId = new Meter.Id(id.getName(), Tags.empty(), id.getBaseUnit(), id.getDescription(), id.getType());
        return newId;
    }
}