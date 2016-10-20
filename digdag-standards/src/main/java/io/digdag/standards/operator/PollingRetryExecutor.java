package io.digdag.standards.operator;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

public class PollingRetryExecutor
{
    private static final Duration DEFAULT_MIN_INTERVAL = Duration.ofSeconds(1);
    private static final Duration DEFAULT_MAX_INTERVAL = Duration.ofSeconds(30);

    private static final String DEFAULT_ERROR_MESSAGE = "Operation failed";
    private static final Object[] DEFAULT_ERROR_MESSAGE_PARAMETERS = {};
    private static final Predicate<Exception> DEAFULT_RETRY_PREDICATE = e -> true;

    private static Logger logger = LoggerFactory.getLogger(PollingRetryExecutor.class);

    private static final String DONE = "done";
    private static final String RETRY = "retry";

    private final Config state;
    private final String stateKey;

    private final Duration minRetryInterval;
    private final Duration maxRetryInterval;

    private final Predicate<Exception> retryPredicate;

    private final String errorMessage;
    private final Object[] errorMessageParameters;

    private PollingRetryExecutor(Config state, String stateKey, Duration minRetryInterval, Duration maxRetryInterval, Predicate<Exception> retryPredicate, String errorMessage, Object... errorMessageParameters)
    {
        this.state = Objects.requireNonNull(state, "state");
        this.stateKey = Objects.requireNonNull(stateKey, "stateKey");
        this.minRetryInterval = Objects.requireNonNull(minRetryInterval, "minRetryInterval");
        this.maxRetryInterval = Objects.requireNonNull(maxRetryInterval, "maxRetryInterval");
        this.retryPredicate = Objects.requireNonNull(retryPredicate, "retryPredicate");
        this.errorMessage = Objects.requireNonNull(errorMessage, "errorMessage");
        this.errorMessageParameters = Objects.requireNonNull(errorMessageParameters, "errorMessageParameters");
    }

    public static PollingRetryExecutor pollingRetryExecutor(Config state, String stateKey)
    {
        return new PollingRetryExecutor(state, stateKey, DEFAULT_MIN_INTERVAL, DEFAULT_MAX_INTERVAL, DEAFULT_RETRY_PREDICATE, DEFAULT_ERROR_MESSAGE, DEFAULT_ERROR_MESSAGE_PARAMETERS);
    }

    public PollingRetryExecutor withErrorMessage(String errorMessage, Object... errorMessageParameters)
    {
        return new PollingRetryExecutor(state, stateKey, minRetryInterval, maxRetryInterval, retryPredicate, errorMessage, errorMessageParameters);
    }

    public PollingRetryExecutor retryUnless(Predicate<Exception> predicate)
    {
        return retryIf(e -> !predicate.test(e));
    }

    private PollingRetryExecutor retryIf(Predicate<Exception> retryPredicate)
    {
        return new PollingRetryExecutor(state, stateKey, minRetryInterval, maxRetryInterval, retryPredicate, errorMessage, errorMessageParameters);
    }

    public PollingRetryExecutor withRetryInterval(Duration minRetryInterval, Duration maxRetryInterval)
    {
        return withRetryInterval(DurationInterval.of(minRetryInterval, maxRetryInterval));
    }

    public PollingRetryExecutor withRetryInterval(DurationInterval retryInterval)
    {
        return new PollingRetryExecutor(state, stateKey, retryInterval.min(), retryInterval.max(), retryPredicate, errorMessage, errorMessageParameters);
    }

    public PollingRetryExecutor withMinRetryInterval(Duration minRetryInterval)
    {
        return withRetryInterval(minRetryInterval, maxRetryInterval);
    }

    public PollingRetryExecutor withMaxRetryInterval(Duration maxRetryInterval)
    {
        return withRetryInterval(minRetryInterval, maxRetryInterval);
    }

    public void runOnce(Runnable f)
    {
        Config retryState = state.getNestedOrSetEmpty(stateKey);

        boolean done = retryState.get(DONE, boolean.class, false);

        if (done) {
            return;
        }

        run(() -> {
            f.run();
            return null;
        });

        retryState.set(DONE, true);
    }

    public void run(Runnable f)
    {
        run(() -> {
            f.run();
            return null;
        });
    }

    public <T> T run(Callable<T> f)
    {
        Config retryState = state.getNestedOrSetEmpty(stateKey);

        T result;

        try {
            result = f.call();
        }
        catch (Exception e) {
            if (!retryPredicate.test(e)) {
                throw new TaskExecutionException(e, TaskExecutionException.buildExceptionErrorConfig(e));
            }

            int retry = retryState.get(RETRY, int.class, 0);
            retryState.set(RETRY, retry + 1);
            int interval = (int) Math.min(minRetryInterval.getSeconds() * Math.pow(2, retry), maxRetryInterval.getSeconds());
            String formattedErrorMessage = String.format(errorMessage, errorMessageParameters);
            logger.warn("{}: retrying in {} seconds", formattedErrorMessage, interval, e);
            throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
        }

        // Clear retry state
        retryState.remove(RETRY);

        return result;
    }
}
