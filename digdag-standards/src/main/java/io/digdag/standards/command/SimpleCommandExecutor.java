package io.digdag.standards.command;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;
import static io.digdag.util.Workspace.workspacePath;

public class SimpleCommandExecutor
    implements CommandExecutor
{
    @Inject
    public SimpleCommandExecutor()
    { }

    public Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
        throws IOException
    {
        // TODO set TZ environment variable
        return pb.directory(workspacePath(projectPath, request).toFile()).start();
    }
}
