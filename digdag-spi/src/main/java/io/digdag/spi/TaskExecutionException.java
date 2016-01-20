package io.digdag.spi;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;

public class TaskExecutionException
        extends RuntimeException
{
    private final Optional<Config> error;
    private final Optional<Integer> retryInterval;

    public TaskExecutionException(Throwable cause,
            Config error,
            Optional<Integer> retryInterval)
    {
        super(cause);
        this.error = Optional.of(error);
        this.retryInterval = retryInterval;
    }

    public TaskExecutionException(int retryInterval)
    {
        this.error = Optional.absent();
        this.retryInterval = Optional.of(retryInterval);
    }

    public Optional<Config> getError()
    {
        return error;
    }

    public Optional<Integer> getRetryInterval()
    {
        return retryInterval;
    }
}
