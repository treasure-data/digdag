package io.digdag.util;

import java.util.List;
import java.util.ArrayList;
import java.nio.file.Path;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

public abstract class BaseOperator
        implements Operator
{
    protected final OperatorContext context;
    protected final TaskRequest request;
    protected final Workspace workspace;

    public BaseOperator(OperatorContext context)
    {
        this.context = context;
        this.request = context.getTaskRequest();
        this.workspace = Workspace.ofTaskRequest(context.getProjectPath(), request);
    }

    @Override
    public TaskResult run()
    {
        RetryControl retry = RetryControl.prepare(request.getConfig(), request.getLastStateParams(), false);
        try {
            try {
                return runTask();
            }
            finally {
                workspace.close();
            }
        }
        catch (RuntimeException ex) {
            // Propagate polling TaskExecutionException instances
            if (ex instanceof TaskExecutionException) {
                TaskExecutionException tex = (TaskExecutionException) ex;
                boolean isPolling = !tex.isError();
                if (isPolling) {
                    // TODO: reset retry state params
                    throw tex;
                }
            }

            boolean doRetry = retry.evaluate();
            if (doRetry) {
                // Remove command status so that pollable command executors can run tasks on retry
                Config nextStateParams = retry.getNextRetryStateParams().remove("commandStatus");
                throw TaskExecutionException.ofNextPollingWithCause(ex,
                        retry.getNextRetryInterval(),
                        ConfigElement.copyOf(nextStateParams));
            }
            else {
                throw ex;
            }
        }
    }

    public abstract TaskResult runTask();
}
