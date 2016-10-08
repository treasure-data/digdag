package io.digdag.core.workflow;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import java.nio.file.Path;
import java.util.List;

public class StoreOperatorFactory
        implements OperatorFactory
{
    @Inject
    public StoreOperatorFactory()
    { }

    public String getType()
    {
        return "store";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new StoreOperator(context);
    }

    private static class StoreOperator
            implements Operator
    {
        private final TaskRequest request;

        public StoreOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();
            Config storeParams = params.getNestedOrGetEmpty("_command");
            List<ConfigKey> resetParams = params.getListOrEmpty("reset", ConfigKey.class);

            return TaskResult.defaultBuilder(request)
                .resetStoreParams(resetParams)
                .storeParams(storeParams)
                .build();
        }
    }
}
