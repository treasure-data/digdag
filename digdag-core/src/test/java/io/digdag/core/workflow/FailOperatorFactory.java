package io.digdag.core.workflow;

import java.nio.file.Path;

import com.google.inject.Inject;
import io.digdag.spi.TaskExecutionContext;
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
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new FailOperator(workspacePath, request);
    }

    private static class FailOperator
            implements Operator
    {
        private final Path workspacePath;
        private final TaskRequest request;

        public FailOperator(Path workspacePath, TaskRequest request)
        {
            this.workspacePath = workspacePath;
            this.request = request;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            Config params = request.getConfig();

            String message = params.get("_command", String.class);

            throw new RuntimeException(message);
        }
    }
}
