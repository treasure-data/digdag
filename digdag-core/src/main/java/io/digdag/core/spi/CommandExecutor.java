package io.digdag.core.spi;

import java.io.IOException;
import com.google.common.base.Optional;

public interface CommandExecutor
{
    Process start(
            Optional<RevisionInfo> archiveRevision,
            ProcessBuilder pb)
        throws IOException;
}
