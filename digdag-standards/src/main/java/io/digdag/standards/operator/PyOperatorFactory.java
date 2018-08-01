package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Maps;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.CommandExecutorContext;
import io.digdag.spi.CommandExecutorRequest;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.standards.command.AbstractCommandWaitOperatorFactory;
import io.digdag.standards.command.ProcessCommandExecutor;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PyOperatorFactory
        extends AbstractCommandWaitOperatorFactory
        implements OperatorFactory
{
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
    public PyOperatorFactory(Config systemConfig, CommandExecutor exec, ObjectMapper mapper)
    {
        super("py", systemConfig);
        this.exec = exec;
        this.mapper = mapper;
    }

    public String getType()
    {
        return "py";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new PyOperator(context);
    }

    private class PyOperator
            extends BaseOperator
    {
        private final Config params;
        private final TaskState state;
        private final int scriptPollInterval;

        public PyOperator(OperatorContext context)
        {
            super(context);
            this.params = request.getConfig().mergeDefault(request.getConfig().getNestedOrGetEmpty(getType()));
            this.state = TaskState.of(request);
            this.scriptPollInterval = PyOperatorFactory.this.getPollInterval(params);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("py"))
                .merge(request.getLastStateParams());  // merge state parameters in addition to regular config

            Config data;
            try {
                data = runCode(params);
            }
            catch (IOException | InterruptedException ex) {
                throw Throwables.propagate(ex);
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(data.getNestedOrGetEmpty("subtask_config"))
                .exportParams(data.getNestedOrGetEmpty("export_params"))
                .storeParams(data.getNestedOrGetEmpty("store_params"))
                .build();
        }

        private Config runCode(final Config params)
                throws IOException, InterruptedException
        {
            final Config stateParams = state.params();
            final Path projectPath = workspace.getProjectPath();
            final Path workspacePath = workspace.getPath();

            final CommandStatus status;
            if (!stateParams.has("command_status")) {
                // Run the code since command state doesn't exist
                status = runCode(params, projectPath, workspacePath);
            }
            else {
                // Check the status of the code running
                final ObjectNode previousStatusJson = stateParams.get("command_status", ObjectNode.class);
                status = checkCodeState(params, projectPath, workspacePath, previousStatusJson);
            }

            if (status.isFinished()) {
                final int statusCode = status.getStatusCode();
                if (statusCode != 0) {
                    throw new RuntimeException("Python command failed with code " + statusCode);
                }

                final Path outputPath = workspacePath.resolve(status.getIoDirectory()).resolve("output.json");
                try (final InputStream in = Files.newInputStream(outputPath)) {
                    return mapper.readValue(in, Config.class);
                }
                finally {
                    // Remove the polling state after fetching the result so that the result fetch can be retried
                    // without resubmitting the code.
                    stateParams.remove("command_status");
                }
            }
            else {
                stateParams.set("command_status", status);
                throw TaskExecutionException.ofNextPolling(scriptPollInterval, ConfigElement.copyOf(stateParams));
            }
        }

        private CommandStatus runCode(final Config params,
                final Path projectPath,
                final Path workspacePath)
                throws IOException, InterruptedException
        {
            final Path tempDir = workspace.createTempDir(String.format("digdag-py-%d-", request.getTaskId()));
            final Path inputPath = tempDir.resolve("input.json"); // absolute
            final Path outputPath = tempDir.resolve("output.json"); // absolute
            final Path runnerPath = tempDir.resolve("runner.py"); // absolute

            final String script;
            final List<String> cmdline;
            final Path cwd = workspace.getPath(); // absolute
            final String python = params.get("python", String.class, "python");

            if (params.has("_command")) {
                final String command = params.get("_command", String.class);
                script = runnerScript;
                cmdline = ImmutableList.of(String.format("cat %s | %s - %s %s %s",
                        cwd.relativize(runnerPath).toString(), // relative
                        python,
                        command,
                        cwd.relativize(inputPath).toString(), // relative
                        cwd.relativize(outputPath).toString())); // relative
            }
            else {
                script = params.get("script", String.class);
                cmdline = ImmutableList.of(String.format("cat %s | %s - %s %s",
                        cwd.relativize(runnerPath).toString(), // relative
                        python,
                        cwd.relativize(inputPath).toString(), // relative
                        cwd.relativize(outputPath).toString())); // relative
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
            ProcessCommandExecutor.collectEnvironmentVariables(environments, context.getPrivilegedVariables());

            final CommandExecutorContext context = buildCommandExecutorContext(projectPath, workspacePath);
            final CommandExecutorRequest request = buildCommandExecutorRequest(context, cwd, tempDir, environments, cmdline);
            return exec.run(context, request);

            // TaskExecutionException could not be thrown here to poll the task by non-blocking for process-base
            // command executor. Because they will be bounded by the _instance_ where the command was executed
            // first.
        }

        private CommandStatus checkCodeState(final Config params,
                final Path projectPath,
                final Path workspacePath,
                final ObjectNode previousStatusJson)
                throws IOException, InterruptedException
        {
            final CommandExecutorContext context = buildCommandExecutorContext(projectPath, workspacePath);
            return exec.poll(context, previousStatusJson);
        }

        private CommandExecutorContext buildCommandExecutorContext(final Path projectPath, final Path workspacePath)
        {
            return CommandExecutorContext.builder()
                    .localProjectPath(projectPath)
                    .workspacePath(workspacePath)
                    .taskRequest(this.request)
                    .build();
        }

        private CommandExecutorRequest buildCommandExecutorRequest(final CommandExecutorContext context,
                final Path cwd,
                final Path tempDir,
                final Map<String, String> environments,
                final List<String> cmdline)
        {
            final Path directory = context.getLocalProjectPath().relativize(cwd); // relative
            final Path ioDirectory = context.getLocalProjectPath().relativize(tempDir); // relative
            return CommandExecutorRequest.builder()
                    .directory(directory)
                    .environments(environments)
                    .command(cmdline)
                    .ioDirectory(ioDirectory)
                    .build();
        }
    }
}
