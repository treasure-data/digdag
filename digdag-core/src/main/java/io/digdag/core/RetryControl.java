package io.digdag.core;

public class RetryControl
{
    public static RetryControl prepare(ConfigSource config, ConfigSource stateParams, boolean enableDefaultByDefault)
    {
        return new RetryControl(stateParams);
    }

    private final ConfigSource stateParams;

    private RetryControl(ConfigSource stateParams)
    {
        this.stateParams = stateParams;
    }

    public int getNextRetryInterval()
    {
        // TODO
        return 0;
    }

    public ConfigSource getNextRetryStateParams()
    {
        // TODO
        return stateParams;
    }

    public boolean evaluate(ConfigSource error)  // TODO error class
    {
        // TODO
        return false;
    }
}
