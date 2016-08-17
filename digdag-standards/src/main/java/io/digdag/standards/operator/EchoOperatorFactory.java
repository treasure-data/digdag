package io.digdag.standards.operator;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;

import java.nio.file.Path;

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
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new EchoOperator(request);
    }

    private static class EchoOperator
            implements Operator
    {
        private final TaskRequest request;

        public EchoOperator(TaskRequest request)
        {
            this.request = request;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            Config params = request.getConfig();

            String message = params.get("_command", String.class);

            System.out.println(message);

            return TaskResult.empty(request);
        }
    }
}
