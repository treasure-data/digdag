package io.digdag.core.agent;

import java.nio.file.Path;
import java.io.IOException;
import io.digdag.spi.TaskRequest;

public interface WorkspaceManager
{
    public interface WithWorkspaceAction<T>
    {
        public T run(Path workspacePath);
    }

    <T> T withExtractedArchive(TaskRequest request, WithWorkspaceAction<T> func)
        throws IOException;
}
