package io.digdag.standards.operator;

import java.nio.file.Path;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;

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

            String message = params.get("_command", String.class, "");

            Config errorParams = params.getFactory().create();
            errorParams.set("message", message);

            throw new TaskExecutionException(message, ConfigElement.copyOf(errorParams));
        }
    }
}
