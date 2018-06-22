package io.digdag.standards.operator;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.digdag.client.config.ConfigElement;
import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.standards.command.ProcessCommandExecutor;
import io.digdag.util.AbstractWaitOperatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandStatus;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.client.config.Config;
import io.digdag.util.BaseOperator;

public class PyOperatorFactory
        extends AbstractWaitOperatorFactory
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
            //ClogProxyOutputStream cmdout = new ClogProxyOutputStream(clog); // TODO
            final Config stateParams = state.params().getNestedOrGetEmpty("polling_code");
            final Path projectPath = workspace.getProjectPath();
            final CommandStatus status;
            if (!stateParams.has("commandId")) {
                final String inFile = workspace.createTempFile("digdag-py-in-", ".tmp"); // relative path
                final String outFile = workspace.createTempFile("digdag-py-out-", ".tmp"); // relative path
                final String runnerFile = workspace.createTempFile("digdag-py-runner-", ".py"); // relative path

                final String script;
                final List<String> cmdline;
                final String python = params.get("python", String.class, "python");

                if (params.has("_command")) {
                    String command = params.get("_command", String.class);
                    script = runnerScript;
                    cmdline = ImmutableList.<String>builder()
                            .add("bash")
                            .add("-c")
                            .add(String.format("cat %s | %s - %s %s %s", runnerFile, python, command, inFile, outFile))
                            .build();
                }
                else {
                    script = params.get("script", String.class);
                    cmdline = ImmutableList.<String>builder()
                            .add("bash")
                            .add("-c")
                            .add(String.format("cat %s | %s - %s %s %s", runnerFile, python, inFile, outFile))
                            .build();
                }

                // Write params to inFile
                try (final OutputStream out = workspace.newOutputStream(inFile)) {
                    mapper.writeValue(out, ImmutableMap.of("params", params));
                }

                // Write script content to runnerFile
                try (final Writer writer = new BufferedWriter(new OutputStreamWriter(workspace.newOutputStream(runnerFile)))) {
                    writer.write(script);
                }

                Map<String, String> environments = System.getenv();
                ProcessCommandExecutor.collectEnvironmentVariables(environments, context.getPrivilegedVariables());

                final Path workspacePath = workspace.getPath();
                status = exec.run(projectPath, workspacePath, request,
                        environments,
                        cmdline,
                        ImmutableMap.<String, CommandExecutorContent>builder()
                                .put("in_content", CommandExecutorContent.create(workspacePath, inFile))
                                .put("runner_content", CommandExecutorContent.create(workspacePath, runnerFile))
                                .build(),
                        CommandExecutorContent.create(workspacePath, outFile) // out_content
                );

                // TaskExecutionException could not be thrown here to poll the task by non-blocking for process-base
                // command executor. Because they will be bounded by the _instance_ where the command was executed
                // first.
            }
            else {
                // Poll tasks by non-blocking
                final String commandId = stateParams.get("commandId", String.class);
                final Config executorState = stateParams.getNestedOrGetEmpty("executorState");
                status = exec.poll(projectPath, request, commandId, executorState);
            }

            //status.getCommandOutput().writeTo(clog); // TODO
            if (status.isFinished()) {
                CommandExecutorContent outputContent = status.getOutputContent();
                return mapper.readValue(workspace.getFile(outputContent.getName()), Config.class); // TODO stop this
            }
            else {
                stateParams.set("commandId", status.getCommandId());
                stateParams.set("executorState", status.getExecutorState());
                throw TaskExecutionException.ofNextPolling(scriptPollInterval, ConfigElement.copyOf(stateParams));
            }
        }
    }
}
