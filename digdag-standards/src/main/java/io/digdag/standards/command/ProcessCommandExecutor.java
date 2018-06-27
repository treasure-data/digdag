package io.digdag.standards.command;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.TaskRequest;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class ProcessCommandExecutor
    implements CommandExecutor
{
    private static Pattern VALID_ENV_KEY = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");

    private final CommandLogger clog;

    @Inject
    public ProcessCommandExecutor(final CommandLogger clog)
    {
        this.clog = clog;
    }

    @Override
    @Deprecated
    public abstract Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    protected abstract Process startProcess(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    @Override
    public CommandStatus run(final Path projectPath, final Path workspacePath, final TaskRequest request,
            final Map<String, String> environments, final List<String> cmdline,
            final Map<String, CommandExecutorContent> inputContents,
            final CommandExecutorContent outputContent)
            throws IOException, InterruptedException
    {
        final ProcessBuilder pb = new ProcessBuilder(cmdline);
        pb.directory(projectPath.toFile());
        pb.redirectErrorStream(true);
        pb.environment().putAll(environments);

        final Process p = startProcess(projectPath, request, pb);

        // copy stdout to System.out and logger
        clog.copyStdout(p, System.out);

        // Need waiting and blocking. Because the process is running on a single instance.
        // The command task could not be taken by other digdag-servers on other instances.
        final int statusCode = p.waitFor();
        if (statusCode != 0) {
            throw new RuntimeException("Python command failed with code " + statusCode);
        }

        final CommandExecutorContent newOutputContent = CommandExecutorContent.create(workspacePath, outputContent.getName());
        return ProcessCommandStatus.of(statusCode, newOutputContent);
    }

    @Override
    public CommandStatus poll(final Path projectPath, final Path workspacePath, final TaskRequest request,
            final String commandId, final Config executorState)
    {
        throw new UnsupportedOperationException("this method is never called.");
    }

    public static void collectEnvironmentVariables(final Map<String, String> env, final PrivilegedVariables variables)
    {
        for (String name : variables.getKeys()) {
            if (!VALID_ENV_KEY.matcher(name).matches()) {
                throw new ConfigException("Invalid _env key name: " + name);
            }
            env.put(name, variables.get(name));
        }
    }

    public static boolean isValidEnvKey(String key)
    {
        return VALID_ENV_KEY.matcher(key).matches();
    }
}