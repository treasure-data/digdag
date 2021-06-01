package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandStatus;
import java.io.IOException;
import java.nio.file.Path;

public class SimpleCommandExecutor
        implements CommandExecutor
{
    private final CommandLogger clog;

    @Inject
    public SimpleCommandExecutor(final CommandLogger clog)
    {
        this.clog = clog;
    }

    @Override
    public boolean isBlocking()
    {
        return true;
    }

    @Override
    public CommandStatus run(final CommandContext context, final CommandRequest request)
            throws IOException
    {
        final ProcessBuilder pb = new ProcessBuilder(request.getCommandLine());
        final Path workingDirectory = context.getLocalProjectPath().resolve(request.getWorkingDirectory()).normalize();
        pb.directory(workingDirectory.toFile());
        pb.redirectErrorStream(true);
        pb.environment().putAll(request.getEnvironments());

        // TODO set TZ environment variable
        final Process p = pb.start();

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

        return SimpleCommandStatus.of(p, request.getIoDirectory());
    }

    /**
     * This method is never called. The status of the task that is executed by the executor cannot be
     * polled by non-blocking.
     */
    @Override
    public CommandStatus poll(final CommandContext context, final ObjectNode previousStatusJson)
            throws IOException
    {
        throw new UnsupportedOperationException("This method should not be called.");
    }
}
