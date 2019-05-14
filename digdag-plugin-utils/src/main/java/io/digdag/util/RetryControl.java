package io.digdag.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;

public class RetryControl
{
    public static RetryControl prepare(Config config, Config stateParams, boolean enableByDefault)
    {
        return new RetryControl(config, stateParams, enableByDefault);
    }

    private enum RetryIntervalType
    {
        CONSTATNT("constant"), EXPONENTIAL("exponential");

        String type;

        private RetryIntervalType(final String type)
        {
            this.type = type;
        }

        public static RetryIntervalType find(String value)
        {
            for (RetryIntervalType r : RetryIntervalType.values()) {
                if (r.type.equalsIgnoreCase(value)) {
                    return r;
                }
            }
            throw new ConfigException("Invalid retry_interval_type: " + value);
        }
    }

    private final Config stateParams;
    private final int retryLimit;
    private final int retryCount;
    private final int retryInterval;
    private final Optional<Integer> maxRetryInterval;
    private final RetryIntervalType retryIntervalType;

    private RetryControl(Config config, Config stateParams, boolean enableByDefault)
    {

        this.stateParams = stateParams;
        this.retryCount = stateParams.get("retry_count", int.class, 0);

        final JsonNode retryNode = config.getInternalObjectNode().get("_retry");
        try {
            if (retryNode == null) {  // No _retry description. default.
                this.retryLimit = enableByDefault ? 3 : 0;
                this.retryInterval = 0;
                this.maxRetryInterval = Optional.absent();
                this.retryIntervalType = RetryIntervalType.CONSTATNT;
            }
            else if (retryNode.isNumber() || retryNode.isTextual()) {  // Only limit is set.
                // If set as variable ${..}, the value become text. Here uses Config.get(type, key)
                // to get retryLimit so that text is also accepted.
                this.retryLimit = config.get("_retry", int.class);
                this.retryInterval = 0;
                this.maxRetryInterval = Optional.absent();
                this.retryIntervalType = RetryIntervalType.CONSTATNT;
            }
            else if (retryNode.isObject()) {  // json format
                Config retry = config.getNested("_retry");
                this.retryLimit = retry.get("limit", int.class);
                this.retryInterval = retry.getOptional("interval", int.class).or(0).intValue();
                this.maxRetryInterval = retry.getOptional("max_interval", int.class);
                this.retryIntervalType = retry.getOptional("interval_type", String.class)
                    .transform(RetryIntervalType::find)
                    .or(RetryIntervalType.CONSTATNT);
            }
            else {  // Unknown format
                throw new ConfigException(String.format("Invalid _retry format:%s", retryNode.toString()));
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
                interval = retryInterval * (int) Math.pow(2, retryCount);
                if (maxRetryInterval.isPresent()) {
                    interval = Math.min(interval, maxRetryInterval.get());
                }
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
