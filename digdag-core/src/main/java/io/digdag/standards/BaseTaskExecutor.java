package io.digdag.standards;

import java.util.List;
import java.util.ArrayList;
import com.google.common.collect.*;
import com.google.common.base.*;
import io.digdag.core.agent.RetryControl;
import io.digdag.core.config.Config;
import io.digdag.core.spi.TaskExecutionException;
import io.digdag.core.spi.TaskExecutor;
import io.digdag.core.spi.TaskRequest;
import io.digdag.core.spi.TaskReport;
import io.digdag.core.spi.TaskResult;
import io.digdag.core.agent.TaskRunner;

public abstract class BaseTaskExecutor
        implements TaskExecutor
{
    protected final TaskRequest request;
    protected Config stateParams;

    protected Config subtaskConfig;
    protected final List<Config> inputs;
    protected final List<Config> outputs;

    public BaseTaskExecutor(TaskRequest request)
    {
        this.request = request;
        this.stateParams = request.getLastStateParams().deepCopy();
        this.subtaskConfig = request.getConfig().getFactory().create();
        this.inputs = new ArrayList<>();
        this.outputs = new ArrayList<>();
    }

    public Config getSubtaskConfig()
    {
        return subtaskConfig;
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
        RetryControl retry = RetryControl.prepare(request.getConfig(), stateParams, true);
        try {
            Config carryParams = runTask();
            return TaskResult.builder()
                .subtaskConfig(subtaskConfig)
                .report(
                    TaskReport.builder()
                    .inputs(ImmutableList.copyOf(inputs))
                    .outputs(ImmutableList.copyOf(outputs))
                    .carryParams(carryParams)
                    .build())
                .build();
        }
        catch (RuntimeException ex) {
            Config error = TaskRunner.makeExceptionError(request.getConfig().getFactory(), ex);
            boolean doRetry = retry.evaluate(error);
            this.stateParams = retry.getNextRetryStateParams();
            if (doRetry) {
                throw new TaskExecutionException(ex, error, Optional.of(retry.getNextRetryInterval()));
            }
            else {
                throw ex;
            }
        }
    }

    public abstract Config runTask();

    @Override
    public Config getStateParams()
    {
        return stateParams;
    }
}
