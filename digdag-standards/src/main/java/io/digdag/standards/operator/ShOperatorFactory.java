package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.util.Maps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.TaskExecutionException;
import io.digdag.spi.TaskResult;
import io.digdag.standards.operator.state.TaskState;
import io.digdag.util.BaseOperator;
import io.digdag.util.CommandOperators;
import io.digdag.util.UserSecretTemplate;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(ShOperatorFactory.class);

    private final CommandExecutor exec;

    @Inject
    public ShOperatorFactory(CommandExecutor exec)
    {
        this.exec = exec;
    }

    public String getType()
    {
        return "sh";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new ShOperator(context);
    }

    @VisibleForTesting
    static Void runCodeForTesting(ShOperator operator, Config state)
    {
        try {
            operator.runCode(state);
            return null;
        }
        catch (IOException | InterruptedException e) {
            throw ThrowablesUtil.propagate(e);
        }
    }

    class ShOperator
            extends BaseOperator
    {
        // TODO extract as config params.
        final int scriptPollInterval = (int) Duration.ofSeconds(10).getSeconds();

        public ShOperator(OperatorContext context)
        {
            super(context);
        }

        @Override
        public TaskResult runTask()
        {
            final Config state = TaskState.of(request).params();
            try {
                runCode(state);
                return TaskResult.empty(request);
            }
            catch (IOException | InterruptedException e) {
                throw ThrowablesUtil.propagate(e);
            }
        }

        private void runCode(final Config state)
                throws IOException, InterruptedException
        {
            final Config params = request.getConfig()
                    .mergeDefault(request.getConfig().getNestedOrGetEmpty("sh"));
            final Path projectPath = workspace.getProjectPath();
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
                    throw new RuntimeException("Command failed with code " + statusCode);
                }
                return;
            }
            else {
                state.set("commandStatus", status);
                throw TaskExecutionException.ofNextPolling(scriptPollInterval, ConfigElement.copyOf(state));
            }
        }

        private List<String> getShell(final Config params, final Boolean winOS){
            // Until digdag ver. 0.9, it was necessary to write ["powershell.exe","-"] to specify shell, 
            // but since ver. 0.10, jobs are not executed unless ["powershell.exe"] is written.
            // To resolve this incompatibility, change the shell specification ["powershell.exe","-"]
            // to ["powershell.exe"].            
            List<String> temp_shell;
            if (params.has("shell")) {
                temp_shell = params.getListOrEmpty("shell", String.class);
                if (temp_shell.get(0).toLowerCase().contains("powershell") && temp_shell.size() == 2){
                    if (temp_shell.get(1).contains("-")){
                        temp_shell.remove(1);
                        logger.info("removed \"-\" from shell specification --- from [\"powershell.exe\",\"-\"] to [\"powershell.exe\"] .");
                    }
                }
            }
            else {
                temp_shell = ImmutableList.of(winOS ? "PowerShell.exe" : "/bin/sh");
            }
            return temp_shell;
        }

        private CommandStatus runCommand(final Config params, final CommandContext commandContext)
                throws IOException, InterruptedException
        {
            final Path tempDir = workspace.createTempDir(String.format("digdag-sh-%d-", request.getTaskId()));
            final Path workingDirectory = workspace.getPath(); // absolute
            final Boolean winOS = isWindowsPlatform();
            final Path runnerPath = tempDir.resolve(winOS ? "runner.ps1" : "runner.sh"); // absolute

            final List<String> shell = getShell(params, winOS);

            final ImmutableList.Builder<String> cmdline = ImmutableList.builder();

            cmdline.addAll(shell);

            cmdline.add(workingDirectory.relativize(runnerPath).toString()); // relative

            final String shScript = UserSecretTemplate.of(params.get("_command", String.class))
                    .format(context.getSecrets());

            final Map<String, String> environments = Maps.newHashMap();
            params.getKeys()
                .forEach(key -> {
                    if (CommandOperators.isValidEnvKey(key)) {
                        JsonNode value = params.get(key, JsonNode.class);
                        String string;
                        if (value.isTextual()) {
                            string = value.textValue();
                        }
                        else {
                            string = value.toString();
                        }
                        environments.put(key, string);
                    }
                    else {
                        logger.trace("Ignoring invalid env var key: {}", key);
                    }
                });

            // Set up process environment according to env config. This can also refer to secrets.
            CommandOperators.collectEnvironmentVariables(environments, context.getPrivilegedVariables());

            // add workspace path to the end of $PATH so that bin/cmd works without ./ at the beginning
            String pathEnv = System.getenv("PATH");
            if (pathEnv == null) {
                pathEnv = workspace.getPath().toString();
            }
            else {
                pathEnv = pathEnv + File.pathSeparator + workspace.getPath().toAbsolutePath().toString();
            }

            // Write script content to runnerPath
            try (Writer writer = Files.newBufferedWriter(runnerPath)) {
                writer.write(shScript);
            }

            final CommandRequest commandRequest = buildCommandRequest(commandContext, workingDirectory, tempDir, environments, cmdline.build());
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

        private boolean isWindowsPlatform()
        {
            final String os = System.getProperty("os.name").toLowerCase();
            if ( os.startsWith("windows")) {
                return true;
            }
            return false;
        }
    }
}
