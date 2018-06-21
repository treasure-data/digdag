package io.digdag.standards.command;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandResult;
import io.digdag.spi.CommandState;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.TaskRequest;
import io.digdag.util.Workspace;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class ProcessCommandExecutor
    implements CommandExecutor<DefaultCommandContext>
{
    private static Pattern VALID_ENV_KEY = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");

    private final CommandLogger clog;
    private final ObjectMapper mapper;

    @Inject
    public ProcessCommandExecutor(final CommandLogger clog, final ObjectMapper mapper)
    {
        this.clog = clog;
        this.mapper = mapper;
    }

    @Override
    @Deprecated
    public abstract Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    protected abstract Process startProcess(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    @Override
    public CommandResult start(final DefaultCommandContext commandContext)
            throws IOException
    {
        final Workspace workspace = commandContext.getWorkspace();
        final OperatorContext operatorContext = commandContext.getOperatorContext();

        final String inFile = workspace.createTempFile("digdag-process-in-", ".tmp");
        final String outFile = workspace.createTempFile("digdag-process-out-", ".tmp");

        try (OutputStream fo = workspace.newOutputStream(inFile)) {
            final Config params = commandContext.getParams();
            mapper.writeValue(fo, ImmutableMap.of("params", params));
        }

        final Path workspacePath = workspace.getPath();
        final List<String> arguments = ImmutableList.<String>builder()
                .addAll(commandContext.getArguments())
                .add(inFile)
                .add(outFile)
                .build();
        final List<String> cmdline = ImmutableList.<String>builder()
                .addAll(commandContext.getCommandLine())
                .addAll(arguments)
                .build();
        final ProcessBuilder pb = new ProcessBuilder(cmdline);
        pb.directory(workspacePath.toFile());
        pb.redirectErrorStream(true);

        // Set up process environment according to env config. This can also refer to secrets.
        final Map<String, String> env = pb.environment();
        collectEnvironmentVariables(env, operatorContext.getPrivilegedVariables());

        final Process p = startProcess(workspacePath, operatorContext.getTaskRequest(), pb);
        final Path outFilePath = workspace.getPath(outFile);
        final ProcessCommandResult result = new ProcessCommandResult(p, outFilePath);

        // feed script to stdin
        try (final Writer writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()))) {
            writer.write(commandContext.getScript());
        }

        // copy stdout to System.out and logger
        clog.copyStdout(p, System.out);

        return result;
    }

    @Override
    public CommandResult get(CommandState state)
            throws IOException
    {
        // process-based command executor doesn't support thie method because it's not easy to implement
        // non-blocking tasks polling.
        throw new UnsupportedOperationException("ProcessCommandExecutor doesn't support non-blocking tasks polling");
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