package io.digdag.core.workflow;

import java.nio.file.Path;

import com.google.inject.Inject;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.client.config.Config;

public class FailOperatorFactory
        implements OperatorFactory
{
    @Inject
    public FailOperatorFactory()
    { }

    public String getType()
    {
        return "fail";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new FailOperator(context);
    }

    private static class FailOperator
            implements Operator
    {
        private final TaskRequest request;

        public FailOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            String message = params.get("_command", String.class);

            throw new RuntimeException(message);
        }
    }
}
