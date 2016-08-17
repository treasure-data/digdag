package io.digdag.standards.operator;

import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
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
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new ShOperator(workspacePath, request);
    }

    private class ShOperator
            extends BaseOperator
    {
        public ShOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask(TaskExecutionContext ctx)
        {
            Config params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("nop"));

            String command = params.get("_command", String.class);
            logger.info("nop: {}", command);

            return TaskResult.empty(request);
        }
    }
}
