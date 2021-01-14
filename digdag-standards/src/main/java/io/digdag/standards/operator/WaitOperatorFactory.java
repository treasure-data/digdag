package io.digdag.standards.operator;

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
        private static Logger logger = LoggerFactory.getLogger(WaitOperator.class);
        private final OperatorContext context;
        private final TaskRequest request;
        private final Workspace workspace;

        private final String WAIT_START_TIME_PARAM = "wait_start_time";

        private WaitOperator(OperatorContext context)
        {
            this.context = context;
            this.request = context.getTaskRequest();
            this.workspace = Workspace.ofTaskRequest(context.getProjectPath(), request);
        }

        public TaskResult run()
        {
            Duration duration;
            try {
                Config config = request.getConfig();
                duration = Durations.parseDuration(config.get("_command", String.class));
                if (duration == null) {
                    throw new ConfigException("Wait duration should be specified");
                }
            }
            catch (RuntimeException re) {
                throw new ConfigException("Invalid configuration", re);
            }

            logger.debug("wait duration: {}", duration);

            Instant now = Instant.now();
            Instant start = request.getLastStateParams()
                    .getOptional(WAIT_START_TIME_PARAM, Long.class)
                    .transform(Instant::ofEpochMilli)
                    .or(now);

            if (now.isAfter(start.plusMillis(duration.toMillis()))) {
                logger.info("wait finished. start:{}", start);
            }
            else {
                // Wait at least 1 second
                long nextPollSecs = Math.max(
                        Duration.between(now, start.plusMillis(duration.toMillis())).getSeconds(),
                        1);
                logger.debug("polling after {}s", nextPollSecs);
                throw TaskExecutionException.ofNextPolling(
                        (int)nextPollSecs,
                        ConfigElement.copyOf(
                                request.getLastStateParams().set(WAIT_START_TIME_PARAM, start.toEpochMilli())));
            }
            return TaskResult.empty(request);
        }
    }
}
