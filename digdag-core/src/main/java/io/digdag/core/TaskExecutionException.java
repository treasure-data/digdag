package io.digdag.core;

import com.google.common.base.Optional;

public class TaskExecutionException
        extends RuntimeException
{
    private final Optional<ConfigSource> error;
    private final Optional<Integer> retryInterval;

    public TaskExecutionException(Throwable cause,
            ConfigSource error,
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

    public Optional<ConfigSource> getError()
    {
        return error;
    }

    public Optional<Integer> getRetryInterval()
    {
        return retryInterval;
    }
}
