package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorContext;
import io.digdag.spi.CommandExecutorRequest;
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
    private final static JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;
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
    public CommandStatus run(final CommandExecutorContext context, final CommandExecutorRequest request)
            throws IOException
    {
        final List<String> commands = Lists.newArrayList("/bin/bash", "-c");
        commands.addAll(request.getCommand());

        final ProcessBuilder pb = new ProcessBuilder(commands);
        pb.directory(context.getLocalProjectPath().toFile());
        pb.redirectErrorStream(true);
        pb.environment().putAll(request.getEnvironments());

        final Process p = startProcess(context.getLocalProjectPath(), context.getTaskRequest(), pb);

        // copy stdout to System.out and logger
        clog.copyStdout(p, System.out);

        // Need waiting and blocking. Because the process is running on a single instance.
        // The command task could not be taken by other digdag-servers on other instances.
        try {
            p.waitFor();
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }

        return createCommandStatus(request.getIoDirectory().toString(), p);
    }

    private CommandStatus createCommandStatus(final String ioDirectory, final Process p)
            throws IOException
    {
        final ObjectNode object = JsonNodeFactory.instance.objectNode();
        object.set("finished", JSON_NODE_FACTORY.booleanNode(true));
        object.set("status_code", JSON_NODE_FACTORY.numberNode(p.exitValue()));
        object.set("io_directory", JSON_NODE_FACTORY.textNode(ioDirectory));
        return DefaultCommandStatus.fromObjectNode(object);
    }

    /**
     * This method could not be used for ProcessCommandExecutor. The status of the task that is executed by the executor
     * cannot be polled by non-blocking.
     */
    @Override
    public CommandStatus poll(final CommandExecutorContext context, final ObjectNode previousStatusJson)
            throws IOException
    {
        throw new UnsupportedOperationException("This method is never called.");
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