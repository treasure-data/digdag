package io.digdag.spi;

import java.io.IOException;
import com.google.common.base.Optional;

public interface CommandExecutor
{
    Process start(TaskRequest request, ProcessBuilder pb)
        throws IOException;
}
