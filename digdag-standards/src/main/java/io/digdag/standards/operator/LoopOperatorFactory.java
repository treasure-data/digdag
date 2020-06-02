package io.digdag.standards.operator;

import java.nio.file.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.digdag.core.Limits;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import static java.util.Locale.ENGLISH;

public class LoopOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(LoopOperatorFactory.class);

    @Inject
    public LoopOperatorFactory()
    { }

    public String getType()
    {
        return "loop";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new LoopOperator(context);
    }

    private static class LoopOperator
            implements Operator
    {
        private final TaskRequest request;

        public LoopOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");

            int count = params.get("count", int.class,
                    params.get("_command", int.class));

            if (count > Limits.maxWorkflowTasks()) {
                throw new ConfigException("Too many loop subtasks. Limit: " + Limits.maxWorkflowTasks());
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
