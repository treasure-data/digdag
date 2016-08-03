package io.digdag.spi;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigElement;

public class TaskExecutionException
        extends RuntimeException
{
    public static ConfigElement buildExceptionErrorConfig(Throwable ex)
    {
        Map<String, String> map = ImmutableMap.of(
                "message", ex.toString(),
                "stacktrace",
                Arrays.asList(ex.getStackTrace())
                .stream()
                .map(it -> it.toString())
                .collect(Collectors.joining(", ")));
        return ConfigElement.ofMap(map);
    }

    public static TaskExecutionException ofNextPolling(int interval, ConfigElement nextStateParams)
    {
        return new TaskExecutionException(interval, nextStateParams);
    }

    private TaskExecutionException(int retryInterval, ConfigElement stateParams)
    {
        super("Retrying this task after "+retryInterval+" seconds");
        this.error = Optional.absent();
        this.retryInterval = Optional.of(retryInterval);
        this.stateParams = Optional.of(stateParams);
    }

    private final Optional<ConfigElement> error;
    private final Optional<Integer> retryInterval;
    private final Optional<ConfigElement> stateParams;

    public TaskExecutionException(Throwable cause, ConfigElement error)
    {
        super(cause);
        this.error = Optional.of(error);
        this.retryInterval = Optional.absent();
        this.stateParams = Optional.absent();
    }

    public TaskExecutionException(String message, ConfigElement error)
    {
        super(message);
        this.error = Optional.of(error);
        this.retryInterval = Optional.absent();
        this.stateParams = Optional.absent();
    }

    public TaskExecutionException(Throwable cause, ConfigElement error, int retryInterval, ConfigElement stateParams)
    {
        super(cause);
        this.error = Optional.of(error);
        this.retryInterval = Optional.of(retryInterval);
        this.stateParams = Optional.of(stateParams);
    }

    public TaskExecutionException(String message, ConfigElement error, int retryInterval, ConfigElement stateParams)
    {
        super(message);
        this.error = Optional.of(error);
        this.retryInterval = Optional.of(retryInterval);
        this.stateParams = Optional.of(stateParams);
    }

    public Optional<Config> getError(ConfigFactory cf)
    {
        return error.transform(it -> it.toConfig(cf));
    }

    public boolean isError() {
        return error.isPresent();
    }

    public Optional<Integer> getRetryInterval()
    {
        return retryInterval;
    }

    public Optional<Config> getStateParams(ConfigFactory cf)
    {
        return stateParams.transform(it -> it.toConfig(cf));
    }
}
