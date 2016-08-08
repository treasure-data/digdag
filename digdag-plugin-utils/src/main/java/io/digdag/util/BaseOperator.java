package io.digdag.util;

import java.util.List;
import java.util.ArrayList;
import java.nio.file.Path;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TaskRequest;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskExecutionException;
import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;

public abstract class BaseOperator
        implements Operator
{
    protected final Path workspacePath;
    protected final Workspace workspace;
    protected final TaskRequest request;

    protected final List<Config> inputs;
    protected final List<Config> outputs;

    public BaseOperator(Path workspacePath, TaskRequest request)
    {
        this.workspacePath = workspacePath;
        this.workspace = new Workspace(workspacePath);
        this.request = request;
        this.inputs = new ArrayList<>();
        this.outputs = new ArrayList<>();
    }

    public void addInput(Config input)
    {
        inputs.add(input);
    }

    public void addOutput(Config output)
    {
        outputs.add(output);
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
                throw new TaskExecutionException(ex,
                        buildExceptionErrorConfig(ex),
                        retry.getNextRetryInterval(),
                        ConfigElement.copyOf(retry.getNextRetryStateParams()));
            }
            else {
                throw ex;
            }
        }
    }

    public abstract TaskResult runTask();
}
