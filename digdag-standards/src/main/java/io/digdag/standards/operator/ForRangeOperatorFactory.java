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

    private final Limits limits;

    @Inject
    public ForRangeOperatorFactory(Limits limits)
    {
        this.limits = limits;
    }

    @Override
    public String getType()
    {
        return "for_range";
    }

    @Override
    public ForRangeOperator newOperator(OperatorContext context)
    {
        return new ForRangeOperator(context, limits);
    }

    static class ForRangeOperator
            implements Operator
    {
        private final TaskRequest request;
        private final Limits limits;

        public ForRangeOperator(OperatorContext context, Limits limits)
        {
            this.request = context.getTaskRequest();
            this.limits = limits;
        }

        @Override
        public TaskResult run()
        {
            Config params = request.getConfig();

            Config doConfig = request.getConfig().getNested("_do");

            Config rangeConfig = params.parseNested("_command");
            long from = rangeConfig.get("from", long.class);
            long to = rangeConfig.get("to", long.class);

            Optional<Long> stepConfig = rangeConfig.getOptional("step", Long.class);
            Optional<Long> slicesConfig = rangeConfig.getOptional("slices", Long.class);

            if (stepConfig.isPresent() && slicesConfig.isPresent()) {
                throw new ConfigException("Setting both step and slices options to for_range is invalid");
            }
            if (!stepConfig.isPresent() && !slicesConfig.isPresent()) {
                throw new ConfigException("step or slices option is required for for_range");
            }
            if (stepConfig.isPresent() && stepConfig.get() <= 0) {
                throw new ConfigException("step option must be same or greater than 1 but got " + stepConfig.get());
            }
            if (slicesConfig.isPresent() && slicesConfig.get() <= 0) {
                throw new ConfigException("slices option must be same or greater than 1 but got " + slicesConfig.get());
            }

            long step = stepConfig.or(() -> (to - from + (slicesConfig.get() - 1)) / slicesConfig.get());
            assert step > 0;

            int index = 0;
            Config generated = doConfig.getFactory().create();
            for (long pos = from; pos < to; pos += step) {
                long rangeFrom = pos;
                long rangeTo = Math.min(pos + step, to);
                Config exportParams = params.getFactory().create();
                exportParams.getNestedOrSetEmpty("range")
                    .set("from", rangeFrom)
                    .set("to", rangeTo)
                    .set("index", index);
                Config subtask = params.getFactory().create();
                subtask.setAll(doConfig);
                subtask.getNestedOrSetEmpty("_export").setAll(exportParams);
                generated.set(
                        buildTaskName(rangeFrom, rangeTo),
                        subtask);
                index++;
            }

            enforceTaskCountLimit(index);

            if (params.has("_parallel")) {
                generated.set("_parallel", params.get("_parallel", JsonNode.class));
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(generated)
                .build();
        }

        private static String buildTaskName(long rangeFrom, long rangeTo)
        {
            return String.format("+range-from=%d&to=%d", rangeFrom, rangeTo);
        }

        private void enforceTaskCountLimit(int size)
        {
            if (size > limits.maxWorkflowTasks()) {
                throw new ConfigException("Too many for_range subtasks (" + size + "). Limit: " + limits.maxWorkflowTasks());
            }
        }
    }
}
