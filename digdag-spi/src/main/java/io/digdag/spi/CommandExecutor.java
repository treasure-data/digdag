package io.digdag.spi;

import java.io.IOException;
import java.nio.file.Path;

public interface CommandExecutor
{
    Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
        throws IOException;
}
