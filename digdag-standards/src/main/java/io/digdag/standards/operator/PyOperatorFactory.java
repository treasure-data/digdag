package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.standards.command.AbstractCommandWaitOperatorFactory;
import io.digdag.standards.command.ProcessCommandExecutor;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
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
                final CommandStatus previousCommandStatus = stateParams.get("command_status", CommandStatus.class);
                status = checkCodeState(params, projectPath, workspacePath, previousCommandStatus);
            }

            if (status.isFinished()) {
                if (!status.getStatusCode().isPresent()) {
                    throw new RuntimeException("Cannot get status code even though the code completed.");
                }

                final int statusCode = status.getStatusCode().get();
                if (statusCode != 0) {
                    throw new RuntimeException("Python command failed with code " + statusCode);
                }

                final CommandExecutorContent outputContent = status.getOutputContent("output");
                try (final InputStream in = outputContent.newInputStream()) {
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
            // Make unique command ID for a task
            final String uniqueCommandId = createUniqueCommandId();
            final String inputFile = createTempFileWithSpecificName(workspacePath, uniqueCommandId + "/input");
            final String outputFile = createTempFileWithSpecificName(workspacePath, uniqueCommandId + "/output");
            final String runnerFile = createTempFileWithSpecificName(workspacePath, uniqueCommandId + "/runner");

            final String script;
            final List<String> cmdline;
            final String python = params.get("python", String.class, "python");
            if (params.has("_command")) {
                final String command = params.get("_command", String.class);
                script = runnerScript;
                cmdline = ImmutableList.of(String.format("cat %s | %s - %s %s %s", runnerFile, python, command, inputFile, outputFile));
            }
            else {
                script = params.get("script", String.class);
                cmdline = ImmutableList.of(String.format("cat %s | %s - %s %s", runnerFile, python, inputFile, outputFile));
            }

            // Write params in inputFile
            try (final OutputStream out = workspace.newOutputStream(inputFile)) {
                mapper.writeValue(out, ImmutableMap.of("params", params));
            }

            // Write script content to runnerFile
            try (final Writer writer = new BufferedWriter(new OutputStreamWriter(workspace.newOutputStream(runnerFile)))) {
                writer.write(script);
            }

            final Map<String, String> environments = System.getenv();
            ProcessCommandExecutor.collectEnvironmentVariables(environments, context.getPrivilegedVariables());

            return exec.run(projectPath, workspacePath, request, environments, cmdline, uniqueCommandId);

            // TaskExecutionException could not be thrown here to poll the task by non-blocking for process-base
            // command executor. Because they will be bounded by the _instance_ where the command was executed
            // first.
        }

        private String createUniqueCommandId()
                throws IOException
        {
            // Generate unique command ID and create the temp directory named with same name of the ID. The ID includes
            // random string that is generated by SecureRandom.
            //
            // Id format: digdag-py-{taskId}-{random string}
            String prefix = String.format("digdag-py-%d-", request.getTaskId());
            return workspace.createTempDir(prefix).substring(".digdag/tmp/".length());
        }

        private String createTempFileWithSpecificName(final Path workspacePath, final String fileName)
                throws IOException
        {
            String name = workspacePath.relativize(workspacePath.resolve(fileName)).toString();
            return workspace.createTempFileWithSpecificName(name);
        }

        private CommandStatus checkCodeState(final Config params,
                final Path projectPath,
                final Path workspacePath,
                final CommandStatus previousCommandStatus)
                throws IOException, InterruptedException
        {
            return exec.poll(projectPath, workspacePath, request, previousCommandStatus);
        }
    }
}
