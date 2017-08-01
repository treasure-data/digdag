package io.digdag.standards.command;

import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class SimpleCommandExecutor
    implements CommandExecutor
{
    @Inject
    public SimpleCommandExecutor()
    { }

    public Process start(Path projectPath, TaskRequest request, ProcessBuilder pb, Map<String, String> environments)
        throws IOException
    {
        // TODO set TZ environment variable
        pb.environment().putAll(environments);
        return pb.start();
    }
}
