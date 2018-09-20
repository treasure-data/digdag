package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Maps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import io.digdag.util.CommandOperators;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PyOperatorFactory
        implements OperatorFactory
{
    private static final String OUTPUT_FILE = "output.json";
    private static Logger logger = LoggerFactory.getLogger(PyOperatorFactory.class);

    private final String runnerScript;

    {
        try (InputStreamReader reader = new InputStreamReader(
                    PyOperatorFactory.class.getResourceAsStream("/digdag/standards/py/runner.py"),
                    StandardCharsets.UTF_8)) {
            runnerScript = CharStreams.toString(reader);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private final CommandExecutor exec;
    private final ObjectMapper mapper;

    @Inject
    public PyOperatorFactory(CommandExecutor exec, ObjectMapper mapper)
    {
        this.exec = exec;
        this.mapper = mapper;
    }

    public String getType()
    {
        return "py";
    }

    @VisibleForTesting
    static Config runCodeForTesting(PyOperator operator, Config state)
    {
        try {
            return operator.runCode(state);
        }
        catch (IOException | InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new PyOperator(context);
    }

    class PyOperator
            extends BaseOperator
    {
        // TODO extract as config params.
        final int scriptPollInterval = (int) Duration.ofSeconds(3).getSeconds();

        public PyOperator(OperatorContext context)
        {
            super(context);
        }

        @Override
        public TaskResult runTask()
        {
            final Config data;
            try {
                data = runCode(TaskState.of(request).params());
            }
            catch (IOException | InterruptedException e) {
                throw Throwables.propagate(e);
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(data.getNestedOrGetEmpty("subtask_config"))
                .exportParams(data.getNestedOrGetEmpty("export_params"))
                .storeParams(data.getNestedOrGetEmpty("store_params"))
                .build();
        }

        private Config runCode(final Config state)
                throws IOException, InterruptedException
        {
            final Config params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("py"));
            final Path projectPath = workspace.getProjectPath(); // absolute
            final CommandContext commandContext = buildCommandContext(projectPath);

            final CommandStatus status;
            if (!state.has("commandStatus")) {
                // Run the code since command state doesn't exist
                status = runCommand(params, commandContext);
            }
            else {
                // Check the status of the running command
                final ObjectNode previousStatusJson = state.get("commandStatus", ObjectNode.class);
                status = exec.poll(commandContext, previousStatusJson);
            }

            if (status.isFinished()) {
                final int statusCode = status.getStatusCode();
                if (statusCode != 0) {
                    // Remove the polling state after fetching the result so that the result fetch can be retried
                    // without resubmitting the code.
                    state.remove("commandStatus");
                    throw new RuntimeException("Python command failed with code " + statusCode);
                }

                final Path outputPath = commandContext.getLocalProjectPath().resolve(status.getIoDirectory()).resolve(OUTPUT_FILE);
                try (final InputStream in = Files.newInputStream(outputPath)) {
                    return mapper.readValue(in, Config.class);
                }
            }
            else {
                state.set("commandStatus", status);
                throw TaskExecutionException.ofNextPolling(scriptPollInterval, ConfigElement.copyOf(state));
            }
        }

        private CommandStatus runCommand(final Config params, final CommandContext commandContext)
                throws IOException, InterruptedException
        {
            final Path tempDir = workspace.createTempDir(String.format("digdag-py-%d-", request.getTaskId()));
            final Path workingDirectory = workspace.getPath(); // absolute
            final Path inputPath = tempDir.resolve("input.json"); // absolute
            final Path outputPath = tempDir.resolve(OUTPUT_FILE); // absolute
            final Path runnerPath = tempDir.resolve("runner.py"); // absolute

            final String script;
            final List<String> cmdline;

            if (params.has("_command")) {
                final String methodName = params.get("_command", String.class);
                script = runnerScript;
                cmdline = ImmutableList.of("python",
                        workingDirectory.relativize(runnerPath).toString(), // relative
                        methodName,
                        workingDirectory.relativize(inputPath).toString(), // relative
                        workingDirectory.relativize(outputPath).toString()); // relative
            }
            else {
                script = params.get("script", String.class);
                cmdline = ImmutableList.of("python",
                        workingDirectory.relativize(runnerPath).toString(), // relative
                        workingDirectory.relativize(inputPath).toString(), // relative
                        workingDirectory.relativize(outputPath).toString()); // relative
            }

            // Write params in inputPath
            try (final OutputStream out = Files.newOutputStream(inputPath)) {
                mapper.writeValue(out, ImmutableMap.of("params", params));
            }

            // Write script content to runnerPath
            try (final Writer writer = Files.newBufferedWriter(runnerPath)) {
                writer.write(script);
            }

            final Map<String, String> environments = Maps.newHashMap();
            environments.putAll(System.getenv());
            CommandOperators.collectEnvironmentVariables(environments, context.getPrivilegedVariables());

            final CommandRequest commandRequest = buildCommandRequest(commandContext, workingDirectory, tempDir, environments, cmdline);
            return exec.run(commandContext, commandRequest);

            // TaskExecutionException could not be thrown here to poll the task by non-blocking for process-base
            // command executor. Because they will be bounded by the _instance_ where the command was executed
            // first.
        }

        private CommandContext buildCommandContext(final Path projectPath)
        {
            return CommandContext.builder()
                    .localProjectPath(projectPath)
                    .taskRequest(this.request)
                    .build();
        }

        private CommandRequest buildCommandRequest(final CommandContext commandContext,
                final Path workingDirectory,
                final Path tempDir,
                final Map<String, String> environments,
                final List<String> cmdline)
        {
            final Path projectPath = commandContext.getLocalProjectPath();
            final Path relativeWorkingDirectory = projectPath.relativize(workingDirectory); // relative
            final Path ioDirectory = projectPath.relativize(tempDir); // relative
            return CommandRequest.builder()
                    .workingDirectory(relativeWorkingDirectory)
                    .environments(environments)
                    .commandLine(cmdline)
                    .ioDirectory(ioDirectory)
                    .build();
        }
    }
}
