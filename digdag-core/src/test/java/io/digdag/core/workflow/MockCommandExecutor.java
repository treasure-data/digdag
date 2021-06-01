package io.digdag.core.workflow;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandRequest;
import io.digdag.spi.CommandStatus;
import java.io.IOException;

public class MockCommandExecutor
    implements CommandExecutor
{
    @Inject
    public MockCommandExecutor()
    { }

    @Override
    public boolean isBlocking()
    {
        return true;
    }

    @Override
    public CommandStatus run(CommandContext context, CommandRequest request)
            throws IOException
    {
        final ProcessBuilder pb = new ProcessBuilder(request.getCommandLine());
        pb.directory(request.getWorkingDirectory().normalize().toAbsolutePath().toFile());
        pb.redirectErrorStream(true);
        pb.environment().putAll(request.getEnvironments());

        // TODO set TZ environment variable
        final Process p = pb.start();

        // Need waiting and blocking. Because the process is running on a single instance.
        // The command task could not be taken by other digdag-servers on other instances.
        try {
            p.waitFor();
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }

        return new CommandStatus() {
            @Override
            public boolean isFinished()
            {
                return true;
            }

            @Override
            public int getStatusCode()
            {
                return p.exitValue();
            }

            @Override
            public String getIoDirectory()
            {
                return request.getIoDirectory().toString();
            }

            @Override
            public ObjectNode toJson()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public CommandStatus poll(CommandContext context, ObjectNode previousStatusJson)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
