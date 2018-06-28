package io.digdag.util;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.util.DurationParam;

import java.time.Duration;
import java.util.Locale;

import static io.digdag.util.Durations.formatDuration;

public abstract class AbstractWaitOperatorFactory
{
    // TODO: exponential backoff
    static final int JOB_STATUS_API_POLL_INTERVAL = 5;

    private static final Duration DEFAULT_MIN_POLL_INTERVAL = Duration.ofSeconds(30);
    private static final Duration DEFAULT_MAX_POLL_INTERVAL = Duration.ofDays(2);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMinutes(10);

    private static final String POLL_INTERVAL_FORMAT = "config.%s.poll_interval";
    private static final String MIN_POLL_INTERVAL_FORMAT = "config.%s.min_poll_interval";
    private static final String MAX_POLL_INTERVAL_FORMAT = "config.%s.max_poll_interval";

    private final Duration minPollInterval;
    private final Duration maxPollInterval;
    private final Duration pollInterval;

    protected AbstractWaitOperatorFactory(final String operatorType, Config systemConfig)
    {
        final String pollIntervalKey = String.format(Locale.ENGLISH, POLL_INTERVAL_FORMAT, operatorType);
        final String minPollIntervalKey = String.format(Locale.ENGLISH, MIN_POLL_INTERVAL_FORMAT, operatorType);
        final String maxPollIntervalKey = String.format(Locale.ENGLISH, MAX_POLL_INTERVAL_FORMAT, operatorType);

        this.minPollInterval = systemConfig.getOptional(minPollIntervalKey, DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(DEFAULT_MIN_POLL_INTERVAL);
        if (minPollInterval.getSeconds() < 0 || minPollInterval.getSeconds() > Integer.MAX_VALUE) {
            throw new ConfigException("invalid configuration value: " + minPollIntervalKey);
        }

        this.maxPollInterval = systemConfig.getOptional(maxPollIntervalKey, DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(DEFAULT_MAX_POLL_INTERVAL);
        if (maxPollInterval.getSeconds() < minPollInterval.getSeconds() || maxPollInterval.getSeconds() > Integer.MAX_VALUE) {
            throw new ConfigException("invalid configuration value: " + maxPollIntervalKey);
        }

        this.pollInterval = systemConfig.getOptional(pollIntervalKey, DurationParam.class)
                .transform(DurationParam::getDuration)
                .or(max(minPollInterval, DEFAULT_POLL_INTERVAL));
        if (pollInterval.getSeconds() < minPollInterval.getSeconds() || pollInterval.getSeconds() > maxPollInterval.getSeconds()) {
            throw new ConfigException("invalid configuration value: " + pollIntervalKey);
        }
    }

    private Duration max(Duration a, Duration b)
    {
        return a.compareTo(b) < 0 ? a : b;
    }

    public int getPollInterval(Config params)
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
