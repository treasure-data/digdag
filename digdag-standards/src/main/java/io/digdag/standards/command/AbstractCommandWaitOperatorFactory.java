package io.digdag.standards.command;

import io.digdag.client.config.Config;
import io.digdag.util.AbstractWaitOperatorFactory;

import java.time.Duration;

public abstract class AbstractCommandWaitOperatorFactory
        extends AbstractWaitOperatorFactory
{
    private static final Duration DEFAULT_MIN_POLL_INTERVAL = Duration.ofSeconds(3);
    private static final Duration DEFAULT_MAX_POLL_INTERVAL = Duration.ofHours(24);
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMinutes(10);

    protected AbstractCommandWaitOperatorFactory(final String operatorType, final Config systemConfig)
    {
        super(operatorType, systemConfig);
    }

    @Override
    protected Duration getDefaultMinPollInterval()
    {
        return DEFAULT_MIN_POLL_INTERVAL;
    }

    @Override
    protected Duration getDefaultMaxPollInterval()
    {
        return DEFAULT_MAX_POLL_INTERVAL;
    }

    @Override
    protected Duration getDefaultPollInterval()
    {
        return DEFAULT_POLL_INTERVAL;
    }
}
