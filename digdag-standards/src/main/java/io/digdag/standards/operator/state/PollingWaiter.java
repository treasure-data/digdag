package io.digdag.standards.operator.state;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.util.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

public class PollingWaiter
{
    private static final DurationInterval DEFAULT_POLL_INTERVAL = DurationInterval.of(Duration.ofSeconds(1), Duration.ofSeconds(30));

    private static Logger logger = LoggerFactory.getLogger(PollingWaiter.class);

    private static final String DEFAULT_ERROR_MESSAGE = "Operation failed";
    private static final Object[] DEFAULT_ERROR_MESSAGE_PARAMETERS = {};

    private static final String RESULT = "result";
    private static final String ITERATION = "iteration";
    private static final String OPERATION = "operation";

    private final TaskState state;
    private final String stateKey;

    private final DurationInterval pollInterval;

    private final String waitMessage;
    private final Object[] waitMessageParameters;

    private PollingWaiter(TaskState state, String stateKey, DurationInterval pollInterval, String waitMessage, Object... waitMessageParameters)
    {
        this.state = Objects.requireNonNull(state, "state");
        this.stateKey = Objects.requireNonNull(stateKey, "stateKey");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        this.waitMessage = Objects.requireNonNull(waitMessage, "waitMessage");
        this.waitMessageParameters = Objects.requireNonNull(waitMessageParameters, "waitMessageParameters");
    }

    public static PollingWaiter pollingWaiter(TaskState state, String stateKey)
    {
        return new PollingWaiter(
                state,
                stateKey,
                DEFAULT_POLL_INTERVAL,
                DEFAULT_ERROR_MESSAGE,
                DEFAULT_ERROR_MESSAGE_PARAMETERS);
    }

    public PollingWaiter withWaitMessage(String waitMessage, Object... waitMessageParameters)
    {
        return new PollingWaiter(
                state,
                stateKey,
                pollInterval,
                waitMessage,
                waitMessageParameters);
    }

    public PollingWaiter withPollInterval(DurationInterval retryInterval)
    {
        return new PollingWaiter(
                state,
                stateKey,
                retryInterval,
                waitMessage,
                waitMessageParameters);
    }

    public <T> T awaitOnce(Class<T> type, Operation<Optional<T>> f)
    {
        TaskState pollState = state.nestedState(stateKey);
        T result = pollState.params().get(RESULT, type, null);
        if (result != null) {
            return result;
        }
        result = await(f);
        pollState.params().set(RESULT, result);
        return result;
    }

    public <T> T await(Operation<Optional<T>> f)
    {
        TaskState pollState = state.nestedState(stateKey);

        TaskState operationState = pollState.nestedState(OPERATION);

        Optional<T> result;

        try {
            result = f.perform(operationState);
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }

        if (!result.isPresent()) {
            int iteration = pollState.params().get(ITERATION, int.class, 0);
            pollState.params().set(ITERATION, iteration + 1);
            int interval = (int) Math.min(pollInterval.min().getSeconds() * Math.pow(2, iteration), pollInterval.max().getSeconds());
            String formattedErrorMessage = String.format(waitMessage, waitMessageParameters);
            logger.info("{}: checking again in {}", formattedErrorMessage, Durations.formatDuration(Duration.ofSeconds(interval)));
            throw pollState.pollingTaskExecutionException(interval);
        }

        pollState.params().remove(OPERATION);
        pollState.params().remove(ITERATION);

        return result.get();
    }
}
