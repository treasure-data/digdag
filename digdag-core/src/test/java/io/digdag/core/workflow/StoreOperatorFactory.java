package io.digdag.core.workflow;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigKey;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
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
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new StoreOperator(request);
    }

    private static class StoreOperator
            implements Operator
    {
        private final TaskRequest request;

        public StoreOperator(TaskRequest request)
        {
            this.request = request;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
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
