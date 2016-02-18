package io.digdag.spi;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;

public class TaskExecutionException
        extends RuntimeException
{
    private final Optional<Config> error;
    private final Optional<Integer> retryInterval;
    private final Optional<Config> stateParams;

    public TaskExecutionException(Throwable cause, Config error)
    {
        super(cause);
        this.error = Optional.of(error);
        this.retryInterval = Optional.absent();
        this.stateParams = Optional.absent();
    }

    public TaskExecutionException(int retryInterval, Config stateParams)
    {
        super("Retrying this task after "+retryInterval+" seconds");
        this.error = Optional.absent();
        this.retryInterval = Optional.of(retryInterval);
        this.stateParams = Optional.of(stateParams);
    }

    public TaskExecutionException(Throwable cause, Config error, int retryInterval, Config stateParams)
    {
        super(cause);
        this.error = Optional.of(error);
        this.retryInterval = Optional.of(retryInterval);
        this.stateParams = Optional.of(stateParams);
    }

    public TaskExecutionException(Config error, int retryInterval, Config stateParams)
    {
        super("Retrying this task after "+retryInterval+" seconds");
        this.error = Optional.of(error);
        this.retryInterval = Optional.of(retryInterval);
        this.stateParams = Optional.of(stateParams);
    }

    public Optional<Config> getError()
    {
        return error;
    }

    public Optional<Config> getStateParams()
    {
        return stateParams;
    }

    public Optional<Integer> getRetryInterval()
    {
        return retryInterval;
    }
}
