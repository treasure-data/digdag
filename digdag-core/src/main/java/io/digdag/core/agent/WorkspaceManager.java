package io.digdag.core.agent;

import java.nio.file.Path;
import java.io.IOException;
import com.google.common.base.Optional;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.StorageFile;

public interface WorkspaceManager
{
    public interface ArchiveProvider
    {
        public Optional<StorageFile> open() throws IOException;
    }

    public interface WithWorkspaceAction<T>
    {
        public T run(Path workspacePath);
    }

    <T> T withExtractedArchive(TaskRequest request, ArchiveProvider archiveProvider, WithWorkspaceAction<T> func)
        throws IOException;
}
