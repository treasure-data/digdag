package io.digdag.standards.operator;

import java.nio.file.Path;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;

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
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new IfOperator(request);
    }

    private static class IfOperator
            implements Operator
    {
        private final TaskRequest request;

        public IfOperator(TaskRequest request)
        {
            this.request = request;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
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
