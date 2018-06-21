package io.digdag.standards.operator;

import java.util.List;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandState;
import io.digdag.spi.OperatorContext;
import io.digdag.util.AbstractWaitOperatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.CommandResult;
import io.digdag.standards.command.DefaultCommandContext;
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

        private Config runCode(Config params)
                throws IOException, InterruptedException
        {
            final CommandResult result = runCode(params, "polling_command");

            final boolean done = result.isFinished();
            // Remove the poll command state after fetching the result so that the result fetch can be retried without
            // resubmitting the command.
            state.params().remove("polling_command");

            // If the query condition was not fulfilled, go back to sleep.
            if (!done) {
                throw state.pollingTaskExecutionException(scriptPollInterval);
            }

            // The code condition was fulfilled, we're done.
            final int ecode = result.getExitCode();
            if (ecode != 0) {
                throw new RuntimeException("Python command failed with code " + ecode);
            }
            return result.getTaskResultData(mapper, Config.class);
        }

        private CommandResult runCode(final Config params, final String key)
                throws IOException, InterruptedException
        {
            final CommandState commandState = state.params().get(key, CommandState.class, CommandState.empty());
            final Optional<String> commandId = commandState.commandId();

            final CommandResult result;
            if (!commandId.isPresent()) {
                String script;
                List<String> args;

                if (params.has("_command")) {
                    String command = params.get("_command", String.class);
                    script = runnerScript;
                    args = ImmutableList.of(command);
                }
                else {
                    script = params.get("script", String.class);
                    args = ImmutableList.of();
                }

                final String python = params.get("python", String.class, "python");
                List<String> cmdline = ImmutableList.<String>builder()
                        .add(python).add("-")  // script is fed from stdin
                        .addAll(args)
                        .build();

                final CommandContext commandContext = new DefaultCommandContext(params, script, cmdline, context, workspace);
                result = exec.start(commandContext); // TODO error handling like job duplicated?

                state.params().set("polling_command", commandState.withCommandId(result.getCommandId()));

                // TaskExecutionException could not be thrown here to poll the task by non-blocking for process-base
                // command executor. Because they will be bounded by the _instance_ where the command was executed
                // first.
            }
            else {
                // non-blocking task polling
                result = exec.get(commandState);
            }

            return result;
        }
    }
}
