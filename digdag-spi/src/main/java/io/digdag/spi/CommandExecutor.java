package io.digdag.spi;

import java.io.IOException;
import java.nio.file.Path;
import com.google.common.base.Optional;

public interface CommandExecutor
{
    Process start(Path archivePath, TaskRequest request, ProcessBuilder pb)
        throws IOException;
}
