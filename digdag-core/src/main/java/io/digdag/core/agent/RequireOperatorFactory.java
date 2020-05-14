package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.core.agent.TaskCallbackApi.ProjectIdentifier;
import io.digdag.core.repository.ResourceLimitExceededException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static java.util.Locale.ENGLISH;

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
    public Operator newOperator(OperatorContext context)
    {
        return new RequireOperator(context, callback);
    }

    private static class RequireOperator
            extends BaseOperator
    {
        private final TaskCallbackApi callback;
        private ConfigFactory cf;

        private RequireOperator(OperatorContext context, TaskCallbackApi callback)
        {
            super(context);
            this.callback = callback;
            this.cf = request.getConfig().getFactory();
        }

        @Override
        public TaskResult runTask()
        {
            Config config = request.getConfig();
            String workflowName = config.get("_command", String.class);
            Instant instant = config.get("session_time", Instant.class);
            boolean ignoreFailure = config.get("ignore_failure", boolean.class, false);
            Optional<String> retryAttemptName = config.getOptional("retry_attempt_name", String.class);
            Config overrideParams = config.getNestedOrGetEmpty("params");
            try {
                ProjectIdentifier projectIdentifier = makeProjectIdentifier();

                StoredSessionAttempt attempt = callback.startSession(
                        context,
                        request.getSiteId(),
                        projectIdentifier,
                        workflowName,
                        instant,
                        retryAttemptName,
                        overrideParams);

                boolean isDone = attempt.getStateFlags().isDone();
                if (isDone) {
                    if (!ignoreFailure && !attempt.getStateFlags().isSuccess()) {
                        // ignore_failure is false and the attempt is in error state. Make this operator failed.
                        throw new TaskExecutionException(String.format(ENGLISH,
                                    "Dependent workflow failed. Session id: %d, attempt id: %d",
                                    attempt.getSessionId(), attempt.getId()));
                    }
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
            catch (ResourceLimitExceededException ex) {
                throw new TaskExecutionException(ex);
            }
        }


        /**
         * Make ProjectIdentifier from parameters.
         * @return
         */
        private ProjectIdentifier makeProjectIdentifier()
        {
            Config config = request.getConfig();
            Config localConfig = request.getLocalConfig();
            int projectId = config.get("project_id", int.class);
            Optional<Integer> projectIdParam = localConfig.getOptional("project_id", Integer.class);
            Optional<String> projectNameParam = localConfig.getOptional("project_name", String.class);

            if (projectIdParam.isPresent() && projectNameParam.isPresent()) {
                throw new ConfigException("Both project_id and project_name can't be set");
            }
            ProjectIdentifier projectIdentifier;
            if (projectNameParam.isPresent()) {
                projectIdentifier = ProjectIdentifier.ofName(projectNameParam.get());
            }
            else if (projectIdParam.isPresent()) {
                projectIdentifier = ProjectIdentifier.ofId(projectIdParam.get());
            }
            else {
                projectIdentifier = ProjectIdentifier.ofId(projectId);
            }
            return projectIdentifier;
        }
    }
}
