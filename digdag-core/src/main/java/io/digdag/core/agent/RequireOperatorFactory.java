package io.digdag.core.agent;

import java.util.List;
import java.util.stream.Collectors;
import java.time.Instant;
import java.time.ZoneId;
import java.nio.file.Path;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskReport;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.core.agent.RetryControl;
import io.digdag.core.session.SessionStateFlags;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;

public class RequireOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(RequireOperatorFactory.class);

    private final TaskCallbackApi callback;

    @Inject
    public RequireOperatorFactory(TaskCallbackApi callback)
    {
        this.callback = callback;
    }

    public String getType()
    {
        return "require";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new RequireOperator(callback, request);
    }

    private static class RequireOperator
            implements Operator
    {
        private final TaskCallbackApi callback;
        private final TaskRequest request;
        private ConfigFactory cf;

        public RequireOperator(TaskCallbackApi callback, TaskRequest request)
        {
            this.callback = callback;
            this.request = request;
            this.cf = request.getConfig().getFactory();
        }

        @Override
        public TaskResult run()
        {
            RetryControl retry = RetryControl.prepare(request.getConfig(), request.getLastStateParams(), false);
            boolean isDone;
            try {
                isDone = runTask();
            }
            catch (RuntimeException ex) {
                Config error = OperatorManager.makeExceptionError(request.getConfig().getFactory(), ex);
                boolean doRetry = retry.evaluate();
                if (doRetry) {
                    throw new TaskExecutionException(ex, error,
                            retry.getNextRetryInterval(),
                            retry.getNextRetryStateParams());
                }
                else {
                    throw ex;
                }
            }

            if (isDone) {
                return TaskResult.empty(cf);
            }
            else {
                // TODO use exponential-backoff to calculate retry interval
                throw new TaskExecutionException(1, request.getLastStateParams());
            }
        }

        private boolean runTask()
        {
            Config config = request.getConfig();
            String workflowName = config.get("_command", String.class);
            int repositoryId = config.get("repository_id", int.class);
            Instant instant = config.get("session_time", Instant.class);
            Optional<String> retryAttemptName = config.getOptional("retry_attempt_name", String.class);
            Config overwriteParams = config.getNestedOrGetEmpty("params");
            try {
                SessionStateFlags flags = callback.startSession(
                        request.getSiteId(),
                        repositoryId,
                        workflowName,
                        instant,
                        retryAttemptName,
                        overwriteParams);

                return flags.isDone();
            }
            catch (ResourceNotFoundException ex) {
                throw new ConfigException(ex);
            }
        }
    }
}
