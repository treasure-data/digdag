package io.digdag.standards.operator;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Limits;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static java.util.Locale.ENGLISH;

public class LoopOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(LoopOperatorFactory.class);

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
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new LoopOperator(workspacePath, request);
    }

    private class LoopOperator
            extends BaseOperator
    {
        public LoopOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");

            int count = params.get("count", int.class,
                    params.get("_command", int.class));

            if (count > limits.maxWorkflowTasks()) {
                throw new ConfigException("Too many loop subtasks. Limit: " + limits.maxWorkflowTasks());
            }

            boolean parallel = params.get("_parallel", boolean.class, false);

            Config generated = doConfig.getFactory().create();
            for (int i = 0; i < count; i++) {
                Config subtask = params.getFactory().create();
                subtask.setAll(doConfig);
                subtask.getNestedOrSetEmpty("_export").set("i", i);
                generated.set(
                        String.format(ENGLISH, "+loop-%d", i),
                        subtask);
            }

            if (parallel) {
                generated.set("_parallel", parallel);
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(generated)
                .build();
        }
    }
}
