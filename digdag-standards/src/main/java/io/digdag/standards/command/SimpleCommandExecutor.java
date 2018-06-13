package io.digdag.standards.command;

import java.io.IOException;
import java.nio.file.Path;
import com.google.inject.Inject;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.TaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleCommandExecutor
        extends ProcessCommandExecutor
{
    private static Logger logger = LoggerFactory.getLogger(SimpleCommandExecutor.class);

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
        logger.warn(getClass().getName() + "#start method is deprecated and will be removed.");
        return startProcess(projectPath, request, pb);
    }

    @Override
    public Process startProcess(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException
    {
        // TODO set TZ environment variable
        return pb.start();
    }
}
