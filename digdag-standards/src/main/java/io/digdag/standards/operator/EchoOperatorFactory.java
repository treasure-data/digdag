package io.digdag.standards.operator;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.log.TaskContextLogging;
import io.digdag.core.log.TaskLogger;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

import java.nio.charset.StandardCharsets;

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

            // To store the message to task log
            TaskLogger logger = TaskContextLogging.getContext().getLogger();
            byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
            logger.log(messageBytes, 0, messageBytes.length);

            return TaskResult.empty(request);
        }
    }
}
