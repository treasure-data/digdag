package io.digdag.core.agent;

import java.util.List;
import java.util.stream.Collectors;
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
    public TaskRunner newTaskExecutor(TaskRequest request)
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
            RetryControl retry = RetryControl.prepare(request.getConfig(), stateParams, true);
            try {
                runTask();
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

        private void runTask()
        {
            String workflowName = request.getConfig().get("command", String.class);
            //callback.startSession(
            //        workflowName);
        }

        @Override
        public Config getStateParams()
        {
            return stateParams;
        }
    }
}
