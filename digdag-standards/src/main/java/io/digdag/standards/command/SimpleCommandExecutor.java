package io.digdag.standards.command;

import com.google.inject.Inject;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.TaskRequest;
import java.io.IOException;
import java.nio.file.Path;

public class SimpleCommandExecutor
        extends ProcessCommandExecutor
{
    @Inject
    public SimpleCommandExecutor(final CommandLogger clog)
    {
        super(clog);
    }

    @Override
    @Deprecated
    public Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException
    {
        return startProcess(projectPath, request, pb);
    }

    @Override
    public boolean test(TaskRequest request)
    {
        return true;
    }

    @Override
    public Process startProcess(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException
    {
        // TODO set TZ environment variable
        return pb.start();
    }
}
