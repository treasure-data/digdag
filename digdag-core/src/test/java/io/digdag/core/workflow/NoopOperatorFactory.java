package io.digdag.core.workflow;

import java.nio.file.Path;
import com.google.inject.Inject;
import io.digdag.spi.OperatorContext;
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
    public Operator newOperator(OperatorContext context)
    {
        return new NoopOperator(context);
    }

    private static class NoopOperator
            implements Operator
    {
        private final TaskRequest request;

        public NoopOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
        }

        @Override
        public TaskResult run()
        {
            return TaskResult.empty(request);
        }
    }
}
