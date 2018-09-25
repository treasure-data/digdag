package io.digdag.util;

import com.fasterxml.jackson.databind.JsonNode;
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

        final JsonNode retry = config.getInternalObjectNode().get("_retry");
        try {
            if (retry == null) {  // No _retry description. default.
                this.retryLimit = enableByDefault ? 3 : 0;
                this.retryInterval = 0;
                this.retryIntervalType = RetryIntervalType.CONSTATNT;
            }
            else if (retry.isNumber()) {  // Only limit is set
                this.retryLimit = retry.intValue();
                this.retryInterval = 0;
                this.retryIntervalType = RetryIntervalType.CONSTATNT;
            }
            else if (retry.isObject()) {  // json format
                this.retryLimit = retry.get("limit").intValue();
                if (retry.has("interval")) {
                    this.retryInterval = retry.get("interval").intValue();
                }
                else {
                    this.retryInterval = 0;
                }
                if (retry.has("interval_type")) {
                    this.retryIntervalType = RetryIntervalType.find(retry.get("interval_type").textValue());
                }
                else {
                    this.retryIntervalType = RetryIntervalType.CONSTATNT;
                }
            }
            else {  // Unknown format
                throw new ConfigException("Invalid _retry format");
            }
        }
        catch(NumberFormatException nfe) {
            throw new ConfigException(nfe);
        }
        catch(ConfigException ce) {
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
