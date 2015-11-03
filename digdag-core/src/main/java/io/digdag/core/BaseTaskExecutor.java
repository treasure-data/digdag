package io.digdag.core;

import java.util.List;
import java.util.ArrayList;
import com.google.common.collect.*;
import com.google.common.base.*;
import io.digdag.core.config.Config;
import io.digdag.core.config.MutableConfig;

public abstract class BaseTaskExecutor
        implements TaskExecutor
{
    protected final Config config;
    protected final Config params;
    protected MutableConfig state;

    protected MutableConfig subtaskConfig;
    protected final List<Config> inputs;
    protected final List<Config> outputs;

    public BaseTaskExecutor(Config config, Config params, Config state)
    {
        this.config = config;
        this.params = params;
        this.state = state.mutable();
        this.subtaskConfig = config.getFactory().create();
        this.inputs = new ArrayList<>();
        this.outputs = new ArrayList<>();
    }

    public MutableConfig getSubtaskConfig()
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
        RetryControl retry = RetryControl.prepare(config, state.immutable(), true);
        try {
            Config carryParams = runTask(config, params);
            return TaskResult.builder()
                .subtaskConfig(subtaskConfig.immutable())
                .report(
                    TaskReport.builder()
                    .inputs(ImmutableList.copyOf(inputs))
                    .outputs(ImmutableList.copyOf(outputs))
                    .carryParams(carryParams)
                    .build())
                .build();
        }
        catch (RuntimeException ex) {
            Config error = TaskRunner.makeExceptionError(config.getFactory(), ex);
            boolean doRetry = retry.evaluate(error);
            this.state = retry.getNextRetryStateParams().mutable();
            if (doRetry) {
                throw new TaskExecutionException(ex, error, Optional.of(retry.getNextRetryInterval()));
            }
            else {
                throw ex;
            }
        }
    }

    public abstract Config runTask(Config config, Config params);

    @Override
    public Config getState()
    {
        return state.immutable();
    }
}
