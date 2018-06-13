package io.digdag.standards.command;

import com.google.inject.Inject;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandResult;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.TaskRequest;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
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
    public CommandResult start(final CommandContext commandContext)
            throws IOException
    {
        final OperatorContext operatorContext = commandContext.getOperatorContext();
        final Path workspacePath = commandContext.getWorkspacePath();

        final ProcessBuilder pb = new ProcessBuilder(commandContext.getCommandLine());
        pb.directory(workspacePath.toFile());
        pb.redirectErrorStream(true);

        // Set up process environment according to env config. This can also refer to secrets.
        final Map<String, String> env = pb.environment();
        collectEnvironmentVariables(env, operatorContext.getPrivilegedVariables());

        final Process p = startProcess(workspacePath, operatorContext.getTaskRequest(), pb);
        final ProcessCommandResult result = new ProcessCommandResult(p); // TODO make builder

        // feed script to stdin
        try (final Writer writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()))) {
            writer.write(commandContext.getScript());
        }

        // copy stdout to System.out and logger
        clog.copyStdout(p, System.out);

        return result;
    }

    public static void collectEnvironmentVariables(Map<String, String> env, PrivilegedVariables variables)
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