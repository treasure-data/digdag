package io.digdag.standards.operator;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskExecutionException;

import io.digdag.util.Durations;
import io.digdag.util.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;


public class WaitOperatorFactory
        implements OperatorFactory {

    @Inject
    public WaitOperatorFactory()
    {
    }

    @Override
    public String getType()
    {
        return "wait";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new WaitOperator(context);
    }

    private static class WaitOperator
            implements Operator
    {
        private static final String WAIT_START_TIME_PARAM = "wait_start_time";

        private static final Logger logger = LoggerFactory.getLogger(WaitOperator.class);

        private final OperatorContext context;
        private final TaskRequest request;
        private final Workspace workspace;


        private WaitOperator(OperatorContext context)
        {
            this.context = context;
            this.request = context.getTaskRequest();
            this.workspace = Workspace.ofTaskRequest(context.getProjectPath(), request);
        }

        private Duration duration(Config config)
        {
            Duration duration;
            try {
                duration = Durations.parseDuration(config.get("_command", String.class));
            }
            catch (RuntimeException re) {
                throw new ConfigException("Invalid configuration", re);
            }
            logger.debug("wait duration: {}", duration);
            return duration;
        }

        private boolean blocking(Config config)
        {
            boolean blocking = config.get("blocking", boolean.class, false);
            logger.debug("wait blocking mode: {}", blocking);
            return blocking;
        }

        private Duration pollInterval(Config config)
        {
            Duration pollInterval;
            try {
                Optional<String> pollIntervalStr = config.getOptional("poll_interval", String.class);
                if (!pollIntervalStr.isPresent()) {
                    return null;
                }
                pollInterval = Durations.parseDuration(pollIntervalStr.get());
            }
            catch (RuntimeException re) {
                throw new ConfigException("Invalid configuration", re);
            }
            logger.debug("wait poll_interval: {}", pollInterval);
            return pollInterval;
        }

        public TaskResult run()
        {
            Config config = request.getConfig();

            Duration duration = duration(config);
            boolean blocking = blocking(config);
            Duration pollInterval = pollInterval(config);
            if (blocking && pollInterval != null) {
                throw new ConfigException("poll_interval can't be specified with blocking:true");
            }

            Instant now = Instant.now();
            Instant start = request.getLastStateParams()
                    .getOptional(WAIT_START_TIME_PARAM, Long.class)
                    .transform(Instant::ofEpochMilli)
                    .or(now);

            if (now.isAfter(start.plusMillis(duration.toMillis()))) {
                logger.info("wait finished. start:{}", start);
                return TaskResult.empty(request);
            }

            // Wait at least 1 second
            long waitDurationSeconds = Math.max(
                    Duration.between(now, start.plusMillis(duration.toMillis())).getSeconds(),
                    1);

            if (blocking) {
                logger.debug("waiting for {}s", waitDurationSeconds);
                try {
                    TimeUnit.SECONDS.sleep(waitDurationSeconds);
                    return TaskResult.empty(request);
                }
                catch (InterruptedException e) {
                    // The blocking wait will be restart from the beginning when interrupted.
                    //
                    // There is room to improve this by making the task resume from when interrupted.
                    // But this operator, especially blocking mode, is for development use,
                    // so we'll go with this simple implementation for now.
                    throw new RuntimeException("`wait` operator with blocking mode is interrupted and this will be restart from the beginning of the wait");
                }
            }
            else {
                if (pollInterval != null) {
                    waitDurationSeconds = pollInterval.getSeconds();
                }
                logger.debug("polling after {}s", waitDurationSeconds);
                throw TaskExecutionException.ofNextPolling(
                        (int) waitDurationSeconds,
                        ConfigElement.copyOf(
                                request.getLastStateParams().set(WAIT_START_TIME_PARAM, start.toEpochMilli())));
            }
        }
    }
}
