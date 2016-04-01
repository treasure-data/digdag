package io.digdag.core.agent;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.FileSystems;
import com.google.inject.Inject;
import io.digdag.spi.TaskRequest;

public class NoopWorkspaceManager
    implements WorkspaceManager
{
    @Inject
    public NoopWorkspaceManager()
    { }

    @Override
    public <T> T withExtractedArchive(TaskRequest request, WithWorkspaceAction<T> func)
    {
        Path workspacePath = FileSystems.getDefault().getPath("").toAbsolutePath();
        return func.run(workspacePath);
    }
}
