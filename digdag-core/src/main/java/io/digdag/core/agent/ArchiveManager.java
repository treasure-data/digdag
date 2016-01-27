package io.digdag.core.agent;

import java.nio.file.Path;
import java.io.IOException;
import io.digdag.spi.TaskRequest;

public interface ArchiveManager
{
    public interface WithArchiveAction<T>
    {
        public T run(Path archivePath);
    }

    <T> T withExtractedArchive(TaskRequest request, WithArchiveAction<T> func)
        throws IOException;
}
