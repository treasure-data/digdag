package io.digdag.standards.operator.param;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;

import java.util.List;

public class ParamGetOperatorFactory
        implements OperatorFactory
{
    private final Config systemConfig;
    private final ParamServerClient paramServerClient;

    @Inject
    public ParamGetOperatorFactory(Config systemConfig, ParamServerClient paramServerClient)
    {
        this.systemConfig = systemConfig;
        this.paramServerClient = paramServerClient;
    }

    @Override
    public String getType()
    {
        return "param_get";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new ParamGetOperator(context, paramServerClient);
    }

    private class ParamGetOperator
            extends BaseOperator
    {
        private ParamServerClient paramServerClient;

        public ParamGetOperator(OperatorContext context, ParamServerClient paramServerClient)
        {
            super(context);
            this.paramServerClient = paramServerClient;
        }

        @Override
        public TaskResult runTask()
        {
            Optional<String> paramServerType = systemConfig.getOptional("param_server.database.type", String.class);
            if (!paramServerType.isPresent()) {
                throw new ConfigException("param_server.database.type is required to use this operator.");
            }

            Config params = request.getLocalConfig();
            List<String> keys = params.getKeys();
            if (keys.size() == 0) {
                throw new ConfigException("no key is set.");
            }

            TaskResult taskResult = TaskResult.empty(request);
            Config storeParams = taskResult.getStoreParams();

            for (String key : keys) {
                Optional<String> destKey = params.getOptional(key, String.class);
                if (destKey.isPresent()) {
                    storeParams.set(
                            destKey.get(),
                            paramServerClient.get(key, request.getSiteId()).or("")
                    );
                }
            }

            // To close connection
            paramServerClient.commit();

            return taskResult;
        }
    }
}
