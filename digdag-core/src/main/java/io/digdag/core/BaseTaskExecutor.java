package io.digdag.core;

import java.util.List;
import java.util.ArrayList;
import com.google.common.collect.*;
import com.google.common.base.*;

public abstract class BaseTaskExecutor
        implements TaskExecutor
{
    protected final ConfigSource config;
    protected final ConfigSource params;
    protected ConfigSource state;

    protected final ConfigSource subtaskConfig;
    protected final List<ConfigSource> inputs;
    protected final List<ConfigSource> outputs;

    public BaseTaskExecutor(ConfigSource config, ConfigSource params, ConfigSource state)
    {
        this.config = config;
        this.params = params;
        this.state = state;
        this.subtaskConfig = config.getFactory().create();
        this.inputs = new ArrayList<>();
        this.outputs = new ArrayList<>();
    }

    public ConfigSource getSubtaskConfig()
    {
        return subtaskConfig;
    }

    public void addInput(ConfigSource input)
    {
        inputs.add(input);
    }

    public void addOutput(ConfigSource output)
    {
        outputs.add(output);
    }

    @Override
    public TaskResult run()
    {
        RetryControl retry = RetryControl.prepare(config, state, true);
        try {
            ConfigSource carryParams = runTask(config, params);
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
            ConfigSource error = TaskRunner.makeExceptionError(config.getFactory(), ex);
            boolean doRetry = retry.evaluate(error);
            this.state = retry.getNextRetryStateParams();
            if (doRetry) {
                throw new TaskExecutionException(ex, error, Optional.of(retry.getNextRetryInterval()));
            }
            else {
                throw ex;
            }
        }
    }

    public abstract ConfigSource runTask(ConfigSource config, ConfigSource params);

    @Override
    public ConfigSource getState()
    {
        return state;
    }
}
