package io.digdag.core.workflow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.client.config.Config;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.nio.file.StandardOpenOption.APPEND;

public class EchoOperatorFactory
        implements OperatorFactory
{
    @Inject
    public EchoOperatorFactory()
    { }

    public String getType()
    {
        return "echo";
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new EchoOperator(projectPath, request);
    }

    private static class EchoOperator
            implements Operator
    {
        private final Path projectPath;
        private final TaskRequest request;

        public EchoOperator(Path projectPath, TaskRequest request)
        {
            this.projectPath = projectPath;
            this.request = request;
        }

        @Override
        public TaskResult run(TaskExecutionContext ctx)
        {
            Config params = request.getConfig();

            String message = params.get("_command", String.class);
            Optional<String> appendFile = params.getOptional("append_file", String.class);

            try {
                if (appendFile.isPresent()) {
                    Files.write(projectPath.resolve(appendFile.get()), message.getBytes(UTF_8), CREATE, WRITE, APPEND);
                }
                else {
                    System.out.println(message);
                }
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }

            return TaskResult.empty(request);
        }
    }
}
