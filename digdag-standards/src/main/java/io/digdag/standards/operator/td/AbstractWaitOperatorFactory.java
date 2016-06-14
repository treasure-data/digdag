package io.digdag.standards.operator.td;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

import java.time.Duration;

import static io.digdag.util.Durations.formatDuration;

abstract class AbstractWaitOperatorFactory
{
    private static final Duration DEFAULT_MIN_POLL_INTERVAL = Duration.ofSeconds(30);
    private static final Duration DEFAULT_MAX_POLL_INTERVAL = Duration.ofDays(2);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMinutes(10);

    private static final String POLL_INTERVAL = "config.td.wait.poll_interval";
    private static final String MIN_POLL_INTERVAL = "config.td.wait.min_poll_interval";
    private static final String MAX_POLL_INTERVAL = "config.td.wait.max_poll_interval";

    private final Duration minPollInterval;
    private final Duration maxPollInterval;
    private final Duration pollInterval;

    AbstractWaitOperatorFactory(Config systemConfig)
    {
        this.minPollInterval = systemConfig.getOptional(MIN_POLL_INTERVAL, DurationParam.class)
                .transform(DurationParam::getDuration).or(DEFAULT_MIN_POLL_INTERVAL);
        this.maxPollInterval = systemConfig.getOptional(MAX_POLL_INTERVAL, DurationParam.class)
                .transform(DurationParam::getDuration).or(DEFAULT_MAX_POLL_INTERVAL);
        if (minPollInterval.getSeconds() < 0 || minPollInterval.getSeconds() > Integer.MAX_VALUE) {
            throw new ConfigException("invalid configuration value: " + MIN_POLL_INTERVAL);
        }
        if (maxPollInterval.getSeconds() < minPollInterval.getSeconds() || maxPollInterval.getSeconds() > Integer.MAX_VALUE) {
            throw new ConfigException("invalid configuration value: " + MAX_POLL_INTERVAL);
        }
        this.pollInterval = systemConfig.getOptional(POLL_INTERVAL, DurationParam.class)
                .transform(DurationParam::getDuration).or(max(minPollInterval, DEFAULT_POLL_INTERVAL));
        if (pollInterval.getSeconds() < minPollInterval.getSeconds() || pollInterval.getSeconds() > maxPollInterval.getSeconds()) {
            throw new ConfigException("invalid configuration value: " + POLL_INTERVAL);
        }
    }

    private Duration max(Duration a, Duration b)
    {
        return a.compareTo(b) < 0 ? a : b;
    }

    int getPollInterval(Config params)
    {
        long interval = validatePollInterval(params.get("interval", DurationParam.class, DurationParam.of(pollInterval)).getDuration().getSeconds());
        assert interval >= 0 && interval <= Integer.MAX_VALUE;
        return (int) interval;
    }

    private long validatePollInterval(long interval)
    {
        if (interval < minPollInterval.getSeconds() || interval > maxPollInterval.getSeconds()) {
            throw new ConfigException("poll interval must be at least " + formatDuration(minPollInterval) +
                    " and no greater than " + formatDuration(maxPollInterval));
        }
        return interval;
    }
}
