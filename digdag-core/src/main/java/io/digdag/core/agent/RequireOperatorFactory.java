package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.AttemptStateFlags;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;

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
        return new RequireOperator(workspacePath, callback, request);
    }

    private static class RequireOperator
            extends BaseOperator
    {
        private final TaskCallbackApi callback;
        private final TaskRequest request;
        private ConfigFactory cf;

        private RequireOperator(Path workspacePath, TaskCallbackApi callback, TaskRequest request)
        {
            super(workspacePath, request);
            this.callback = callback;
            this.request = request;
            this.cf = request.getConfig().getFactory();
        }

        @Override
        public TaskResult runTask(TaskExecutionContext ctx)
        {
            Config config = request.getConfig();
            String workflowName = config.get("_command", String.class);
            int projectId = config.get("project_id", int.class);
            Instant instant = config.get("session_time", Instant.class);
            Optional<String> retryAttemptName = config.getOptional("retry_attempt_name", String.class);
            Config overwriteParams = config.getNestedOrGetEmpty("params");
            try {
                AttemptStateFlags flags = callback.startSession(
                        request.getSiteId(),
                        projectId,
                        workflowName,
                        instant,
                        retryAttemptName,
                        overwriteParams);

                boolean isDone = flags.isDone();
                if (isDone) {
                    return TaskResult.empty(cf);
                }
                else {
                    // TODO use exponential-backoff to calculate retry interval
                    throw TaskExecutionException.ofNextPolling(1, ConfigElement.copyOf(request.getLastStateParams()));
                }
            }
            catch (ResourceNotFoundException ex) {
                throw new ConfigException(ex);
            }
        }
    }
}
