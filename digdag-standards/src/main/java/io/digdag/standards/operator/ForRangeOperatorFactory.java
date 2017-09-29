package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.Limits;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForRangeOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(ForRangeOperatorFactory.class);

    @Inject
    public ForRangeOperatorFactory()
    { }

    @Override
    public String getType()
    {
        return "for_range";
    }

    @Override
    public ForRangeOperator newOperator(OperatorContext context)
    {
        return new ForRangeOperator(context);
    }

    static class ForRangeOperator
            implements Operator
    {
        private final TaskRequest request;

        public ForRangeOperator(OperatorContext context)
        {
            this.request = context.getTaskRequest();
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");
            boolean parallel = params.get("_parallel", boolean.class, false);

            Config rangeConfig = params.parseNested("_command");
            long from = rangeConfig.get("from", long.class);
            long until = rangeConfig.get("until", long.class);

            Optional<Long> stepConfig = request.getConfig().getOptional("step", Long.class);
            Optional<Long> countConfig = request.getConfig().getOptional("count", Long.class);

            if (stepConfig.isPresent() && countConfig.isPresent()) {
                throw new ConfigException("Setting both step and count options to for_range is invalid");
            }
            if (!stepConfig.isPresent() && !countConfig.isPresent()) {
                throw new ConfigException("step or count option is required for for_range");
            }

            long step = stepConfig.or(() -> (until - from + (countConfig.get() - 1)) / countConfig.get());

            int count = 0;
            Config generated = doConfig.getFactory().create();
            for (long pos = from; pos < until; pos += step) {
                long rangeFrom = pos;
                long rangeUntil = Math.min(pos + step, until);
                Config exportParams = params.getFactory().create();
                exportParams.getNestedOrSetEmpty("range")
                    .set("from", rangeFrom)
                    .set("until", rangeUntil);
                Config subtask = params.getFactory().create();
                subtask.setAll(doConfig);
                subtask.getNestedOrSetEmpty("_export").setAll(exportParams);
                generated.set(
                        buildTaskName(rangeFrom, rangeUntil),
                        subtask);
                count++;
            }

            enforceTaskCountLimit(count);

            if (parallel) {
                generated.set("_parallel", parallel);
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(generated)
                .build();
        }

        private static String buildTaskName(long rangeFrom, long rangeUntil)
        {
            return String.format("+range-from=%d&until=%d", rangeFrom, rangeUntil);
        }

        private static void enforceTaskCountLimit(int size)
        {
            if (size > Limits.maxWorkflowTasks()) {
                throw new ConfigException("Too many for_range subtasks. Limit: " + Limits.maxWorkflowTasks());
            }
        }
    }
}
