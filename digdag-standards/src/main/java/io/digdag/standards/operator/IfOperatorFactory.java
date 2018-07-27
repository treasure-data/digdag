package io.digdag.standards.operator;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

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
    public IfOperator newOperator(OperatorContext context)
    {
        return new IfOperator(context);
    }

    static class IfOperator
            implements Operator
    {
        private final TaskRequest request;

        public IfOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNestedOrGetEmpty("_do");
            Config elseDoConfig = request.getConfig().getNestedOrGetEmpty("_else_do");
            boolean condition = params.get("_command", boolean.class);
            if(doConfig.isEmpty() && elseDoConfig.isEmpty()){
                throw new ConfigException("Both _do and _else_do are not specified.");
            }
            if (condition) {
                return TaskResult.defaultBuilder(request)
                    .subtaskConfig(doConfig)
                    .build();
            }
            else {
                return TaskResult.defaultBuilder(request)
                    .subtaskConfig(elseDoConfig)
                    .build();
            }
        }
    }
}
