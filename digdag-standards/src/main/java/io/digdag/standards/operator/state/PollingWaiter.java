package io.digdag.standards.operator.state;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.standards.operator.DurationInterval;
import io.digdag.util.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

public class PollingWaiter
{
    private static final DurationInterval DEFAULT_POLL_INTERVAL = DurationInterval.of(Duration.ofSeconds(1), Duration.ofSeconds(30));
    private static final double DEFAULT_NEXT_INTERVAL_EXP_BASE = 2.0;
    private static final Optional<Duration> DEFAULT_TIMEOUT = Optional.absent();

    private static Logger logger = LoggerFactory.getLogger(PollingWaiter.class);

    private static final String DEFAULT_ERROR_MESSAGE = "Operation failed";
    private static final Object[] DEFAULT_ERROR_MESSAGE_PARAMETERS = {};

    private static final String RESULT = "result";
    private static final String ITERATION = "iteration";
    private static final String OPERATION = "operation";
    private static final String START_TIME = "start_time";

    private final TaskState state;
    private final String stateKey;

    private final DurationInterval pollInterval;
    private final double nextIntervalExpBase;
    private final Optional<Duration> timeout;

    private final String waitMessage;
    private final Object[] waitMessageParameters;


    private PollingWaiter(TaskState state, String stateKey, DurationInterval pollInterval, double nextIntervalExpBase, Optional<Duration> timeout, String waitMessage, Object... waitMessageParameters)
    {
        this.state = Objects.requireNonNull(state, "state");
        this.stateKey = Objects.requireNonNull(stateKey, "stateKey");
        this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
        this.nextIntervalExpBase = Objects.requireNonNull(nextIntervalExpBase, "pollInterval");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        this.waitMessage = Objects.requireNonNull(waitMessage, "waitMessage");
        this.waitMessageParameters = Objects.requireNonNull(waitMessageParameters, "waitMessageParameters");
    }

    public static PollingWaiter pollingWaiter(TaskState state, String stateKey)
    {
        return new PollingWaiter(
                state,
                stateKey,
                DEFAULT_POLL_INTERVAL,
                DEFAULT_NEXT_INTERVAL_EXP_BASE,
                DEFAULT_TIMEOUT,
                DEFAULT_ERROR_MESSAGE,
                DEFAULT_ERROR_MESSAGE_PARAMETERS);
    }

    public PollingWaiter withWaitMessage(String waitMessage, Object... waitMessageParameters)
    {
        return new PollingWaiter(
                state,
                stateKey,
                pollInterval,
                nextIntervalExpBase,
                timeout,
                waitMessage,
                waitMessageParameters);
    }

    public PollingWaiter withPollInterval(DurationInterval retryInterval)
    {
        return new PollingWaiter(
                state,
                stateKey,
                retryInterval,
                nextIntervalExpBase,
                timeout,
                waitMessage,
                waitMessageParameters);
    }

    public PollingWaiter withTimeout(Optional<Duration> timeout)
    {
        return new PollingWaiter(
                state,
                stateKey,
                pollInterval,
                nextIntervalExpBase,
                timeout,
                waitMessage,
                waitMessageParameters);
    }

    public PollingWaiter withNextIntervalExpBase(double nextIntervalExpBase)
    {
        return new PollingWaiter(
                state,
                stateKey,
                pollInterval,
                nextIntervalExpBase,
                timeout,
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
        Long startTime = pollState.params().get(START_TIME, Long.class, null);
        if (startTime == null) {
            startTime = System.currentTimeMillis();
            pollState.params().set(START_TIME, startTime);
        }
        TaskState operationState = pollState.nestedState(OPERATION);

        //Check timeout
        long now = System.currentTimeMillis();
        if (timeout.isPresent() && timeout.get().toMillis() <= now - startTime) {
            logger.debug("Timeout happened. startTime:{}, timeout:{}", startTime, timeout.get());
            throw new PollingTimeoutException("Timeout happened");
        }

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
            int interval = (int) Math.min(pollInterval.min().getSeconds() * Math.pow(nextIntervalExpBase, iteration), pollInterval.max().getSeconds());
            String formattedErrorMessage = String.format(waitMessage, waitMessageParameters);
            logger.info("{}: checking again in {}", formattedErrorMessage, Durations.formatDuration(Duration.ofSeconds(interval)));
            throw pollState.pollingTaskExecutionException(interval);
        }

        pollState.params().remove(OPERATION);
        pollState.params().remove(ITERATION);

        return result.get();
    }
}
