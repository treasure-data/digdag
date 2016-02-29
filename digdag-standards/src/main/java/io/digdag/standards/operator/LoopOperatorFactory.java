package io.digdag.standards.operator;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.io.IOException;
import java.nio.file.Path;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import org.immutables.value.Value;
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
    public Operator newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new LoopOperator(archivePath, request);
    }

    private class LoopOperator
            extends BaseOperator
    {
        public LoopOperator(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig().setAllIfNotSet(
                    request.getConfig().getNestedOrGetEmpty("loop"));

            Config doConfig = request.getLocalConfig().getNested("_do");

            int count = params.get("count", int.class,
                    params.get("_command", int.class));

            Config subtasks = doConfig.getFactory().create();
            for (int i = 0; i < count; i++) {
                Config subtask = params.getFactory().create();
                subtask.setAll(doConfig);
                subtask.getNestedOrSetEmpty("_export").set("i", i);
                subtasks.set(
                        String.format(ENGLISH, "+loop-%d", i),
                        subtask);
            }

            System.out.println("generated: " + subtasks);
            return TaskResult.defaultBuilder(request)
                .subtaskConfig(subtasks)
                .build();
        }
    }
}
