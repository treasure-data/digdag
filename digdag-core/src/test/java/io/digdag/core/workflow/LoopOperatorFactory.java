package io.digdag.core.workflow;

import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.digdag.core.Limits;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import static java.util.Locale.ENGLISH;

public class LoopOperatorFactory
        implements OperatorFactory
{
    private final Limits limits;

    @Inject
    public LoopOperatorFactory(Limits limits)
    {
        this.limits = limits;
    }

    public String getType()
    {
        return "loop";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new LoopOperator(context, limits);
    }

    private static class LoopOperator
            implements Operator
    {
        private final TaskRequest request;
        private final Limits limits;

        public LoopOperator(OperatorContext context, Limits limits)
        {
            this.request = context.getTaskRequest();
            this.limits = limits;
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");

            int count = params.get("count", int.class,
                    params.get("_command", int.class));

            if (count > limits.maxWorkflowTasks()) {
                throw new ConfigException("Too many loop subtasks. Limit: " + limits.maxWorkflowTasks());
            }

            Config generated = doConfig.getFactory().create();
            for (int i = 0; i < count; i++) {
                Config subtask = params.getFactory().create();
                subtask.setAll(doConfig);
                subtask.getNestedOrSetEmpty("_export").set("i", i);
                generated.set(
                        String.format(ENGLISH, "+loop-%d", i),
                        subtask);
            }

            if (params.has("_parallel")) {
                generated.set("_parallel", params.get("_parallel", JsonNode.class));
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(generated)
                .build();
        }
    }
}
