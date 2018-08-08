package io.digdag.standards.command;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorContext;
import io.digdag.spi.CommandExecutorRequest;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.CommandStatus;
import java.io.IOException;

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
    public CommandStatus run(final CommandExecutorContext context, final CommandExecutorRequest request)
            throws IOException
    {
        final ProcessBuilder pb = new ProcessBuilder(request.getCommandLine());
        pb.directory(context.getLocalProjectPath().toFile());
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

        return SimpleCommandStatus.of(request.getIoDirectory().toString(), p);
    }

    /**
     * This method is never called. The status of the task that is executed by the executor cannot be
     * polled by non-blocking.
     */
    @Override
    public CommandStatus poll(final CommandExecutorContext context, final ObjectNode previousStatusJson)
            throws IOException
    {
        throw new UnsupportedOperationException("This method should not be called.");
    }
}
