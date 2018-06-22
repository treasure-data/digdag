package io.digdag.core.workflow;

import java.io.IOException;
import java.nio.file.Path;
import com.google.inject.Inject;
import io.digdag.spi.CommandContext;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.CommandState;
import io.digdag.spi.TaskRequest;

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
    public CommandStatus start(CommandContext context)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommandStatus get(CommandState state)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
