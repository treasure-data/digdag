package io.digdag.standards.operator;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

public class EchoOperatorFactory
        implements OperatorFactory
{
    @Inject
    public EchoOperatorFactory()
    { }

    public String getType()
    {
        return "echo";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new EchoOperator(context);
    }

    private static class EchoOperator
            implements Operator
    {
        private final TaskRequest request;

        public EchoOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            String message = params.get("_command", String.class);

            System.out.println(message);

            return TaskResult.empty(request);
        }
    }
}
