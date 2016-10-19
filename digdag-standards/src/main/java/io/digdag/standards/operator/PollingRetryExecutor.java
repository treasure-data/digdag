package io.digdag.standards.operator;

import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

public class PollingRetryExecutor
{
    private static Logger logger = LoggerFactory.getLogger(PollingRetryExecutor.class);

    private static final Duration DEFAULT_MIN_INTERVAL = Duration.ofSeconds(1);
    private static final Duration DEFAULT_MAX_INTERVAL = Duration.ofSeconds(30);

    private static final String DEFAULT_ERROR_MESSAGE = "Operation failed";
    private static final Object[] DEFAULT_ERROR_MESSAGE_PARAMETERS = {};
    private static final List<Predicate<Exception>> DEAFULT_RETRY_PREDICATES = ImmutableList.of();

    private static final String RESULT = "result";
    private static final String DONE = "done";
    private static final String RETRY = "retry";

    private final Config root;
    private final Config state;
    private final String stateKey;

    private final Duration minRetryInterval;
    private final Duration maxRetryInterval;

    private final List<Predicate<Exception>> retryPredicates;

    private final String errorMessage;
    private final Object[] errorMessageParameters;

    private PollingRetryExecutor(
            Config root,
            Config state,
            String stateKey,
            Duration minRetryInterval,
            Duration maxRetryInterval,
            List<Predicate<Exception>> retryPredicates,
            String errorMessage,
            Object... errorMessageParameters)
    {
        this.root = Objects.requireNonNull(root, "root");
        this.state = Objects.requireNonNull(state, "state");
        this.stateKey = Objects.requireNonNull(stateKey, "stateKey");
        this.minRetryInterval = Objects.requireNonNull(minRetryInterval, "minRetryInterval");
        this.maxRetryInterval = Objects.requireNonNull(maxRetryInterval, "maxRetryInterval");
        this.retryPredicates = Objects.requireNonNull(retryPredicates, "retryPredicates");
        this.errorMessage = Objects.requireNonNull(errorMessage, "errorMessage");
        this.errorMessageParameters = Objects.requireNonNull(errorMessageParameters, "errorMessageParameters");
    }

    public static PollingRetryExecutor pollingRetryExecutor(Config root, Config state, String stateKey)
    {
        return new PollingRetryExecutor(
                root,
                state,
                stateKey,
                DEFAULT_MIN_INTERVAL,
                DEFAULT_MAX_INTERVAL,
                DEAFULT_RETRY_PREDICATES,
                DEFAULT_ERROR_MESSAGE,
                DEFAULT_ERROR_MESSAGE_PARAMETERS);
    }

    public PollingRetryExecutor withErrorMessage(String errorMessage, Object... errorMessageParameters)
    {
        return new PollingRetryExecutor(
                root,
                state,
                stateKey,
                minRetryInterval,
                maxRetryInterval,
                retryPredicates,
                errorMessage,
                errorMessageParameters);
    }

    public PollingRetryExecutor retryUnless(Predicate<Exception> predicate)
    {
        return retryIf(e -> !predicate.test(e));
    }

    public <T extends Exception> PollingRetryExecutor retryUnless(Class<T> exceptionClass, Predicate<T> retryPredicate)
    {
        return retryUnless(e -> (exceptionClass.isInstance(e) && retryPredicate.test(exceptionClass.cast(e))));
    }

    public <T extends Exception> PollingRetryExecutor retryIf(Class<T> exceptionClass, Predicate<T> retryPredicate)
    {
        return retryIf(e -> (exceptionClass.isInstance(e) && retryPredicate.test(exceptionClass.cast(e))));
    }

    public PollingRetryExecutor retryIf(Predicate<Exception> retryPredicate)
    {
        List<Predicate<Exception>> newRetryPredicates = ImmutableList.<Predicate<Exception>>builder()
                .addAll(retryPredicates)
                .add(retryPredicate)
                .build();
        return new PollingRetryExecutor(
                root,
                state,
                stateKey,
                minRetryInterval,
                maxRetryInterval,
                newRetryPredicates,
                errorMessage,
                errorMessageParameters);
    }

    public PollingRetryExecutor withRetryInterval(Duration minRetryInterval, Duration maxRetryInterval)
    {
        return withRetryInterval(DurationInterval.of(minRetryInterval, maxRetryInterval));
    }

    public PollingRetryExecutor withRetryInterval(DurationInterval retryInterval)
    {
        return new PollingRetryExecutor(
                root,
                state,
                stateKey,
                retryInterval.min(),
                retryInterval.max(),
                retryPredicates,
                errorMessage,
                errorMessageParameters);
    }

    public PollingRetryExecutor withMinRetryInterval(Duration minRetryInterval)
    {
        return withRetryInterval(minRetryInterval, maxRetryInterval);
    }

    public PollingRetryExecutor withMaxRetryInterval(Duration maxRetryInterval)
    {
        return withRetryInterval(minRetryInterval, maxRetryInterval);
    }

    public void runOnce(Action f)
    {
        runOnce(Void.class, () -> {
            f.run();
            return null;
        });
    }

    public <T> T runOnce(Class<T> type, Callable<T> f)
    {
        Config retryState = state.getNestedOrSetEmpty(stateKey);

        boolean done = retryState.get(DONE, boolean.class, false);

        T result = retryState.get(RESULT, type, null);

        if (done) {
            return result;
        }

        result = run(f);

        retryState.set(RESULT, result);
        retryState.set(DONE, true);

        return result;
    }

    public void run(Action f)
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
        catch (TaskExecutionException e) {
            throw e;
        }
        catch (Exception e) {
            String formattedErrorMessage = String.format(errorMessage, errorMessageParameters);

            if (!retry(e)) {
                logger.warn("{}: giving up", formattedErrorMessage, e);
                throw new TaskExecutionException(e, TaskExecutionException.buildExceptionErrorConfig(e));
            }

            int retryIteration = retryState.get(RETRY, int.class, 0);
            retryState.set(RETRY, retryIteration + 1);
            int interval = (int) Math.min(minRetryInterval.getSeconds() * Math.pow(2, retryIteration), maxRetryInterval.getSeconds());
            logger.warn("{}: retrying in {} seconds", formattedErrorMessage, interval, e);
            throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(root));
        }

        // Clear retry state
        retryState.remove(RETRY);

        return result;
    }

    private boolean retry(Exception e)
    {
        if (retryPredicates.isEmpty()) {
            return true;
        }
        for (Predicate<Exception> retryPredicate : retryPredicates) {
            if (retryPredicate.test(e)) {
                return true;
            }
        }
        return false;
    }

    public static interface Action
    {
        void run()
                throws Exception;
    }
}
