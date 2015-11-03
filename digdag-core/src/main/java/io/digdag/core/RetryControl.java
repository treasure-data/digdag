package io.digdag.core;

import io.digdag.core.config.Config;

public class RetryControl
{
    public static RetryControl prepare(Config config, Config stateParams, boolean enableDefaultByDefault)
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
