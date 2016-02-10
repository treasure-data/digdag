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
import io.digdag.spi.TaskRunner;
import io.digdag.spi.TaskRunnerFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.core.agent.RetryControl;
import io.digdag.core.session.SessionStateFlags;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;

public class RequireTaskRunnerFactory
        implements TaskRunnerFactory
{
    private static Logger logger = LoggerFactory.getLogger(RequireTaskRunnerFactory.class);

    private final TaskCallbackApi callback;

    @Inject
    public RequireTaskRunnerFactory(TaskCallbackApi callback)
    {
        this.callback = callback;
    }

    public String getType()
    {
        return "require";
    }

    @Override
    public TaskRunner newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new RequireTaskRunner(callback, request);
    }

    private class RequireTaskRunner
            implements TaskRunner
    {
        private final TaskCallbackApi callback;
        private final TaskRequest request;
        private Config stateParams;
        private ConfigFactory cf;

        public RequireTaskRunner(TaskCallbackApi callback, TaskRequest request)
        {
            this.callback = callback;
            this.request = request;
            this.stateParams = request.getLastStateParams().deepCopy();
            this.cf = request.getConfig().getFactory();
        }

        @Override
        public TaskResult run()
        {
            RetryControl retry = RetryControl.prepare(request.getConfig(), stateParams, false);
            boolean isDone;
            try {
                isDone = runTask();
            }
            catch (RuntimeException ex) {
                Config error = TaskRunnerManager.makeExceptionError(request.getConfig().getFactory(), ex);
                boolean doRetry = retry.evaluate(error);
                this.stateParams = retry.getNextRetryStateParams();
                if (doRetry) {
                    throw new TaskExecutionException(ex, error, Optional.of(retry.getNextRetryInterval()));
                }
                else {
                    throw ex;
                }
            }

            if (isDone) {
                return TaskResult.builder()
                    .subtaskConfig(cf.create())
                    .report(
                        TaskReport.builder()
                        .inputs(ImmutableList.of())
                        .outputs(ImmutableList.of())
                        .carryParams(cf.create())
                        .build())
                    .build();
            }
            else {
                // TODO use exponential-backoff to calculate retry interval
                throw new TaskExecutionException(1);
            }
        }

        private boolean runTask()
        {
            Config config = request.getConfig();
            String workflowName = config.get("command", String.class);
            int repositoryId = config.get("repository_id", int.class);
            Instant instant = config.get("session_time", Instant.class);
            Optional<String> retryAttemptName = config.getOptional("retry_attempt_name", String.class);
            Config overwriteParams = config.getNestedOrGetEmpty("params");
            SessionStateFlags flags = callback.startSession(
                    repositoryId,
                    workflowName,
                    instant,
                    retryAttemptName,
                    overwriteParams);

            return flags.isDone();
        }

        @Override
        public Config getStateParams()
        {
            return stateParams;
        }
    }
}
