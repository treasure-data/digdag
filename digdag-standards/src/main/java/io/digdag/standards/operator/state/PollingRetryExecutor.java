package io.digdag.standards.operator.state;

import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.operator.DurationInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

public class PollingRetryExecutor
{
    private static Logger logger = LoggerFactory.getLogger(PollingRetryExecutor.class);

    private static final DurationInterval DEFAULT_RETRY_INTERVAL = DurationInterval.of(Duration.ofSeconds(1), Duration.ofSeconds(30));

    private static final String DEFAULT_ERROR_MESSAGE = "Operation failed";
    private static final Object[] DEFAULT_ERROR_MESSAGE_PARAMETERS = {};
    private static final List<Predicate<Exception>> DEAFULT_RETRY_PREDICATES = ImmutableList.of();

    private static final String RESULT = "result";
    private static final String DONE = "done";
    private static final String RETRY = "retry";
    private static final String OPERATION = "operation";

    private final TaskState state;
    private final String stateKey;

    private final DurationInterval retryInterval;

    private final List<Predicate<Exception>> retryPredicates;

    private final String errorMessage;
    private final Object[] errorMessageParameters;

    private PollingRetryExecutor(
            TaskState state,
            String stateKey,
            DurationInterval retryInterval,
            List<Predicate<Exception>> retryPredicates,
            String errorMessage,
            Object... errorMessageParameters)
    {
        this.state = Objects.requireNonNull(state, "state");
        this.stateKey = Objects.requireNonNull(stateKey, "stateKey");
        this.retryInterval = Objects.requireNonNull(retryInterval, "retryInterval");
        this.retryPredicates = Objects.requireNonNull(retryPredicates, "retryPredicates");
        this.errorMessage = Objects.requireNonNull(errorMessage, "errorMessage");
        this.errorMessageParameters = Objects.requireNonNull(errorMessageParameters, "errorMessageParameters");
    }

    public static PollingRetryExecutor pollingRetryExecutor(TaskState state, String stateKey)
    {
        return new PollingRetryExecutor(
                state,
                stateKey,
                DEFAULT_RETRY_INTERVAL,
                DEAFULT_RETRY_PREDICATES,
                DEFAULT_ERROR_MESSAGE,
                DEFAULT_ERROR_MESSAGE_PARAMETERS);
    }

    public PollingRetryExecutor withErrorMessage(String errorMessage, Object... errorMessageParameters)
    {
        return new PollingRetryExecutor(
                state,
                stateKey,
                retryInterval,
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
                state,
                stateKey,
                retryInterval,
                newRetryPredicates,
                errorMessage,
                errorMessageParameters);
    }

    public PollingRetryExecutor withRetryInterval(DurationInterval retryInterval)
    {
        return new PollingRetryExecutor(
                state,
                stateKey,
                retryInterval,
                retryPredicates,
                errorMessage,
                errorMessageParameters);
    }

    public void runOnce(Action f)
    {
        runOnce(Void.class, taskState -> {
            f.perform(taskState);
            return null;
        });
    }

    public <T> T runOnce(Class<T> type, Operation<T> f)
    {
        TaskState retryState = state.nestedState(stateKey);

        boolean done = retryState.params().get(DONE, boolean.class, false);

        T result = retryState.params().get(RESULT, type, null);

        if (done) {
            return result;
        }

        result = run(f);

        retryState.params().set(RESULT, result);
        retryState.params().set(DONE, true);

        return result;
    }

    public void runAction(Action f)
    {
        run(operationState -> {
            f.perform(operationState);
            return null;
        });
    }

    public <T> T run(Operation<T> f)
    {
        TaskState retryState = state.nestedState(stateKey);
        TaskState operationState = retryState.nestedState(OPERATION);

        T result;

        try {
            result = f.perform(operationState);
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

            int retryIteration = retryState.params().get(RETRY, int.class, 0);
            retryState.params().set(RETRY, retryIteration + 1);
            int interval = (int) Math.min(retryInterval.min().getSeconds() * Math.pow(2, retryIteration), retryInterval.max().getSeconds());
            logger.warn("{}: retrying in {} seconds", formattedErrorMessage, interval, e);
            throw state.pollingTaskExecutionException(interval);
        }

        // Clear retry state
        retryState.params().remove(RETRY);

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
}
