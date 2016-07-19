package io.digdag.core.workflow;

import java.nio.file.Path;
import com.google.inject.Inject;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;

public class NoopOperatorFactory
        implements OperatorFactory
{
    @Inject
    public NoopOperatorFactory()
    { }

    public String getType()
    {
        return "noop";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new NoopOperator(request);
    }

    private static class NoopOperator
            implements Operator
    {
        private final TaskRequest request;

        public NoopOperator(TaskRequest request)
        {
            this.request = request;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            return TaskResult.empty(request);
        }
    }
}
