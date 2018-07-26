package io.digdag.core.workflow;

import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorContext;
import io.digdag.spi.CommandExecutorRequest;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskRequest;
import java.io.IOException;
import java.nio.file.Path;

public class MockCommandExecutor
    implements CommandExecutor
{
    @Inject
    public MockCommandExecutor()
    { }

    @Override
    @Deprecated
    public Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
        throws IOException
    {
        return pb.start();
    }

    @Override
    public CommandStatus run(CommandExecutorContext context, CommandExecutorRequest request)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommandStatus poll(CommandExecutorContext context, CommandStatus previousStatus)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
