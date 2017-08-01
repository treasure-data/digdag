package io.digdag.spi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public interface CommandExecutor
{
    Process start(Path projectPath, TaskRequest request, ProcessBuilder pb, Map<String, String> environments)
        throws IOException;
}
