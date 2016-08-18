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
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new EchoOperator(workspacePath, request);
    }

    private static class EchoOperator
            implements Operator
    {
        private final Path workspacePath;
        private final TaskRequest request;

        public EchoOperator(Path workspacePath, TaskRequest request)
        {
            this.workspacePath = workspacePath;
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
                    Files.write(workspacePath.resolve(appendFile.get()), message.getBytes(UTF_8), CREATE, WRITE, APPEND);
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
