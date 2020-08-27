package io.digdag.standards.operator;


import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.util.ParallelControl;
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
        private final OperatorContext context;

        public LoopOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
            this.context = context;
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");

            int count = params.get("count", int.class,
                    params.get("_command", int.class));

            if (count > context.getMaxWorkflowTasks()) {
                throw new ConfigException("Too many loop subtasks. Limit: " + context.getMaxWorkflowTasks());
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

            if (ParallelControl.of(params).isParallel()) {
                generated.set("_parallel", params.get("_parallel", JsonNode.class));
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(generated)
                .build();
        }
    }
}
