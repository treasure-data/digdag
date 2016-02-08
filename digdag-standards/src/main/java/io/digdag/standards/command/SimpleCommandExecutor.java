package io.digdag.standards.command;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;

public class SimpleCommandExecutor
    implements CommandExecutor
{
    // TODO make these parameters configurable
    private final boolean extractArchive = false;
    private final File archiveBuildPath = new File("tmp/");

    @Inject
    public SimpleCommandExecutor()
    {
    }

    public Process start(Path archivePath, TaskRequest request, ProcessBuilder pb)
        throws IOException
    {
        // TODO set TZ environment variable
        return pb.directory(archivePath.toFile()).start();
    }
}
