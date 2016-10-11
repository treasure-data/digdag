package io.digdag.standards.operator;

import autovalue.shaded.com.google.common.common.base.Throwables;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionException;
import io.digdag.util.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Callable;

public class PollingWaiter
{
    private static final Duration DEFAULT_MIN_INTERVAL = Duration.ofSeconds(1);
    private static final Duration DEFAULT_MAX_INTERVAL = Duration.ofSeconds(30);

    private static Logger logger = LoggerFactory.getLogger(PollingWaiter.class);

    private static final String DEFAULT_ERROR_MESSAGE = "Operation failed";
    private static final Object[] DEFAULT_ERROR_MESSAGE_PARAMETERS = {};

    private static final String RESULT = "result";
    private static final String ITERATION = "iteration";

    private final Config state;
    private final String stateKey;

    private final Duration minPollInterval;
    private final Duration maxPollInterval;

    private final String waitMessage;
    private final Object[] waitMessageParameters;

    private PollingWaiter(Config state, String stateKey, Duration minPollInterval, Duration maxPollInterval, String waitMessage, Object... waitMessageParameters)
    {
        this.state = Objects.requireNonNull(state, "state");
        this.stateKey = Objects.requireNonNull(stateKey, "stateKey");
        this.minPollInterval = Objects.requireNonNull(minPollInterval, "minPollInterval");
        this.maxPollInterval = Objects.requireNonNull(maxPollInterval, "maxPollInterval");
        this.waitMessage = Objects.requireNonNull(waitMessage, "waitMessage");
        this.waitMessageParameters = Objects.requireNonNull(waitMessageParameters, "waitMessageParameters");
    }

    public static PollingWaiter pollingWaiter(Config state, String stateKey)
    {
        return new PollingWaiter(state, stateKey, DEFAULT_MIN_INTERVAL, DEFAULT_MAX_INTERVAL, DEFAULT_ERROR_MESSAGE, DEFAULT_ERROR_MESSAGE_PARAMETERS);
    }

    public PollingWaiter withWaitMessage(String waitMessage, Object... waitMessageParameters)
    {
        return new PollingWaiter(state, stateKey, minPollInterval, maxPollInterval, waitMessage, waitMessageParameters);
    }

    public PollingWaiter withPollInterval(Duration minPollInterval, Duration maxPollInterval)
    {
        return withPollInterval(DurationInterval.of(minPollInterval, maxPollInterval));
    }

    public PollingWaiter withPollInterval(DurationInterval retryInterval)
    {
        return new PollingWaiter(state, stateKey, retryInterval.min(), retryInterval.max(), waitMessage, waitMessageParameters);
    }

    public PollingWaiter withMinPollInterval(Duration minPollInterval)
    {
        return withPollInterval(minPollInterval, maxPollInterval);
    }

    public PollingWaiter withMaxPollInterval(Duration maxPollInterval)
    {
        return withPollInterval(minPollInterval, maxPollInterval);
    }

    public <T> T awaitOnce(Class<T> type, Callable<Optional<T>> f)
    {
        Config pollState = state.getNestedOrSetEmpty(stateKey);
        T result = pollState.get(RESULT, type, null);
        if (result != null) {
            return result;
        }
        result = await(f);
        pollState.set(RESULT, result);
        return result;
    }

    public <T> T await(Callable<Optional<T>> f)
    {
        Config pollState = state.getNestedOrSetEmpty(stateKey);

        Optional<T> result;

        try {
            result = f.call();
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        if (!result.isPresent()) {
            int iteration = pollState.get(ITERATION, int.class, 0);
            pollState.set(ITERATION, iteration + 1);
            int interval = (int) Math.min(minPollInterval.getSeconds() * Math.pow(2, iteration), maxPollInterval.getSeconds());
            String formattedErrorMessage = String.format(waitMessage, waitMessageParameters);
            logger.info("{}: checking again in {}", formattedErrorMessage, Durations.formatDuration(Duration.ofSeconds(interval)));
            throw TaskExecutionException.ofNextPolling(interval, ConfigElement.copyOf(state));
        }

        pollState.remove(ITERATION);

        return result.get();
    }
}
