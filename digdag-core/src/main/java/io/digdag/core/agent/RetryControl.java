package io.digdag.core.agent;

import io.digdag.core.spi.config.Config;
import io.digdag.core.workflow.TaskConfig;

public class RetryControl
{
    public static RetryControl prepare(Config config, Config stateParams, boolean enableDefaultByDefault)
    {
        return new RetryControl(stateParams);
    }

    public static RetryControl prepare(TaskConfig config, Config stateParams, boolean enableDefaultByDefault)
    {
        return new RetryControl(stateParams);
    }

    private final Config stateParams;

    private RetryControl(Config stateParams)
    {
        this.stateParams = stateParams;
    }

    public int getNextRetryInterval()
    {
        // TODO
        return 0;
    }

    public Config getNextRetryStateParams()
    {
        // TODO
        return stateParams;
    }

    public boolean evaluate(Config error)  // TODO error class
    {
        // TODO
        return false;
    }
}
