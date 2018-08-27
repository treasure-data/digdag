package io.digdag.util;

import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
//import io.digdag.core.workflow.TaskConfig;

public class RetryControl
{

    public static RetryControl prepare(Config config, Config stateParams, boolean enableByDefault)
    {
        return new RetryControl(config, stateParams, enableByDefault);
    }

    private enum RetryIntervalType {
        CONSTATNT("constant"), EXPONENTIAL("exponential");
        String type;
        private RetryIntervalType(final String type) {
            this.type = type;
        }
        public static RetryIntervalType find(String value) {
            for (RetryIntervalType r : RetryIntervalType.values()) {
                if (r.type.equalsIgnoreCase(value)) {
                    return r;
                }
            }
            throw new ConfigException("Invalid retry_interval_type");
        }
    }

    private final Config stateParams;
    private final int retryLimit;
    private final int retryCount;
    private final int retryInterval;
    private final RetryIntervalType retryIntervalType;

    private RetryControl(Config config, Config stateParams, boolean enableByDefault)
    {
        this.stateParams = stateParams;
        this.retryCount = stateParams.get("retry_count", int.class, 0);

        try {
            String retryStr = config.get("_retry", String.class, "");
            String[] params = {};
            if(! retryStr.equals("")) params = retryStr.split("\\s+", 3);

            if (params.length == 0) {
                this.retryLimit = enableByDefault ? 3 : 0;
            } else {
                this.retryLimit = Integer.parseInt(params[0]);
            }
            if (params.length >= 2) {
                this.retryInterval = Integer.parseInt(params[1]);
            } else {
                this.retryInterval = 0;
            }
            if (params.length >= 3) {
                this.retryIntervalType = RetryIntervalType.find(params[2]);
            } else {
                this.retryIntervalType = RetryIntervalType.CONSTATNT;
            }
        }
        catch(NumberFormatException nfe){
            nfe.printStackTrace();
            throw new ConfigException(nfe);
        }
        catch(ConfigException ce){
            throw ce;
        }
    }

    public int getNextRetryInterval()
    {
        int retryCount = stateParams.get("retry_count", int.class, 0);
        int interval = 0;
        switch(retryIntervalType) {
            case CONSTATNT:
                interval = retryInterval;
                break;
            case EXPONENTIAL:
                interval = retryInterval * (int)Math.pow(2, retryCount);
                break;
        }
        return interval;
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
