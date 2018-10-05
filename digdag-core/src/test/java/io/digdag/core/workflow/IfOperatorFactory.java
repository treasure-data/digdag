package io.digdag.core.workflow;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.*;

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
    public Operator newOperator(OperatorContext context)
    {
        return new IfOperator(context);
    }

    private static class IfOperator
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
