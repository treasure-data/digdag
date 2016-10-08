package io.digdag.standards.operator;

import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

public class NopOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(NopOperatorFactory.class);

    public String getType()
    {
        return "nop";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new NopOperator(context);
    }

    private class NopOperator
            implements Operator
    {
        private final TaskRequest request;

        public NopOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("nop"));

            String command = params.get("_command", String.class);
            logger.info("nop: {}", command);

            return TaskResult.empty(request);
        }
    }
}
