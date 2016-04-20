package io.digdag.core.agent;

import io.digdag.client.config.Config;
import io.digdag.core.workflow.TaskConfig;

public class RetryControl
{
    public static RetryControl prepare(Config config, Config stateParams, boolean enableByDefault)
    {
        return new RetryControl(config, stateParams, enableByDefault);
    }

    public static RetryControl prepare(TaskConfig config, Config stateParams, boolean enableByDefault)
    {
        return new RetryControl(config.getMerged(), stateParams, enableByDefault);
    }

    private final Config stateParams;
    private final int retryLimit;
    private final int retryCount;

    private RetryControl(Config config, Config stateParams, boolean enableByDefault)
    {
        this.stateParams = stateParams;
        this.retryLimit = config.get("_retry", int.class, enableByDefault ? 3 : 0);
        this.retryCount = stateParams.get("retry_count", int.class, 0);
    }

    public int getNextRetryInterval()
    {
        // TODO implement exponential wait
        return 0;
    }

    public Config getNextRetryStateParams()
    {
        return stateParams.deepCopy().set("retry_count", retryCount + 1);
    }

    public boolean evaluate()
    {
        return retryLimit > retryCount;
    }
}
