package io.digdag.spi;

import java.io.IOException;
import java.nio.file.Path;

public interface CommandExecutor<T extends CommandContext>
{
    @Deprecated
    Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    CommandResult start(T context)
            throws IOException;

    // Be used for non-blocking tasks polling
    CommandResult get(CommandState state)
            throws IOException;
}