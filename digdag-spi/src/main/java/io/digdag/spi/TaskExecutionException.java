package io.digdag.spi;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigElement;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Locale.ENGLISH;

/**
 * An exception thrown when an expected exception happens.
 *
 * <p>
 * When an error happens in an operator, it should be wrapped with TaskExecutionException if
 * the cause is expected so that Digdag shows a message for users without verbose stacktrace.
 * The other exception classes are regarded as unexpected and Digdag shows stacktrace for operator
 * developers.
 * </p>
 *
 * <p>
 * TaskExecutionException is also used to let Digdag retry the task later. Use ofNextPolling
 * (if task retries for simple polling) or ofNextPollingWithCause (if task retries to recover
 * from an error) method to retry.
 * </p>
 */
public class TaskExecutionException
        extends RuntimeException
{
    public static ConfigElement buildExceptionErrorConfig(Throwable ex)
    {
        return buildExceptionErrorConfig(formatExceptionMessage(ex), ex);
    }

    public static ConfigElement buildExceptionErrorConfig(String message, Throwable ex)
    {
        Map<String, String> map = ImmutableMap.of(
                "message", message,
                "stacktrace",
                Arrays.asList(ex.getStackTrace())
                .stream()
                .map(it -> it.toString())
                .collect(Collectors.joining(", ")));
        return ConfigElement.ofMap(map);
    }

    private static String formatExceptionMessage(Throwable ex)
    {
        return firstNonEmptyMessage(ex)
            .transform(message -> {
                return String.format(ENGLISH, "%s (%s)", message,
                        ex.getClass().getSimpleName()
                        .replaceFirst("(?:Exception|Error)$", "")
                        .replaceAll("([A-Z]+)([A-Z][a-z])", "$1 $2")
                        .replaceAll("([a-z])([A-Z])", "$1 $2")
                        .toLowerCase());
            })
            .or(() -> ex.toString());
    }

    private static Optional<String> firstNonEmptyMessage(Throwable ex)
    {
        String message = ex.getMessage();
        if (!isNullOrEmpty(message)) {
            return Optional.of(message);
        }
        Throwable cause = ex.getCause();
        if (cause == null) {
            return Optional.absent();
        }
        return firstNonEmptyMessage(cause);
    }

    public static TaskExecutionException ofNextPolling(int interval, ConfigElement nextStateParams)
    {
        return new TaskExecutionException(interval, nextStateParams);
    }

    public static TaskExecutionException ofNextPollingWithCause(Throwable cause, int interval, ConfigElement nextStateParams)
    {
        return new TaskExecutionException(cause, buildExceptionErrorConfig(cause), interval, nextStateParams);
    }

    private TaskExecutionException(int retryInterval, ConfigElement stateParams)
    {
        super("Retrying this task after "+retryInterval+" seconds");
        this.error = Optional.absent();
        this.retryInterval = Optional.of(retryInterval);
        this.stateParams = Optional.of(stateParams);
    }

    private TaskExecutionException(Throwable cause, ConfigElement error, int retryInterval, ConfigElement stateParams)
    {
        super(cause);
        this.error = Optional.of(error);
        this.retryInterval = Optional.of(retryInterval);
        this.stateParams = Optional.of(stateParams);
    }

    private final Optional<ConfigElement> error;
    private final Optional<Integer> retryInterval;
    private final Optional<ConfigElement> stateParams;

    /**
     * Wrap an expected exception to make the task failed.
     */
    public TaskExecutionException(Throwable cause)
    {
        this(formatExceptionMessage(cause), cause);
    }

    /**
     * Wrap an expected exception with a custom well-formatted message to make the task failed.
     */
    public TaskExecutionException(String customMessage, Throwable cause)
    {
        super(customMessage, cause);
        this.error = Optional.of(buildExceptionErrorConfig(customMessage, cause));
        this.retryInterval = Optional.absent();
        this.stateParams = Optional.absent();
    }

    /**
     * Build an expected exception with a simple message to make the task failed.
     */
    public TaskExecutionException(String message)
    {
        this(message, ImmutableMap.of());
    }

    /**
     * Build an expected exception with a simple message and properties to make the task failed.
     */
    public TaskExecutionException(String message, Map<String, String> errorProperties)
    {
        super(message);
        this.error = Optional.of(buildPropertiesErrorConfig(message, errorProperties));
        this.retryInterval = Optional.absent();
        this.stateParams = Optional.absent();
    }

    private static ConfigElement buildPropertiesErrorConfig(String message, Map<String, String> errorProperties)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.putAll(errorProperties);
        builder.put("message", message);
        return ConfigElement.ofMap(builder.build());
    }

    @Deprecated
    public TaskExecutionException(String message, ConfigElement error)
    {
        super(message);
        this.error = Optional.of(error);
        this.retryInterval = Optional.absent();
        this.stateParams = Optional.absent();
    }

    public Optional<Config> getError(ConfigFactory cf)
    {
        return error.transform(it -> it.toConfig(cf));
    }

    public boolean isError()
    {
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
