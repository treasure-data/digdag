package io.digdag.standards.operator;

import java.nio.file.Path;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.util.BaseOperator;

public class IfOperatorFactory
        implements OperatorFactory
{
    @Inject
    public IfOperatorFactory()
    { }

    public String getType()
    {
        return "if";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new IfOperator(workspacePath, request);
    }

    private static class IfOperator
            extends BaseOperator
    {
        public IfOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask(TaskExecutionContext ctx)
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");

            boolean condition = params.get("_command", boolean.class);

            if (condition) {
                return TaskResult.defaultBuilder(request)
                    .subtaskConfig(doConfig)
                    .build();
            }
            else {
                return TaskResult.defaultBuilder(request)
                    .build();
            }
        }
    }
}
