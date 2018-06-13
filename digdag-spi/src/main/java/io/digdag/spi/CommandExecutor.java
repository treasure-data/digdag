package io.digdag.spi;

import java.io.IOException;
import java.nio.file.Path;

public interface CommandExecutor
{
    @Deprecated
    Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    CommandResult start(CommandContext context)
            throws IOException;
}