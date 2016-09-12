package io.digdag.core.agent;

import java.nio.file.Path;
import java.nio.file.Paths;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.spi.TaskRequest;

public class LocalWorkspaceManager
    implements WorkspaceManager
{
    public static final String PROJECT_PATH = "_project_path";

    @Inject
    public LocalWorkspaceManager()
    { }

    @Override
    public <T> T withExtractedArchive(TaskRequest request, ArchiveProvider archiveProvider, WithWorkspaceAction<T> func)
    {
        Path path = request.getConfig().getOptional(PROJECT_PATH, String.class)
            .transform(it -> Paths.get(it).normalize())
            .or(Paths.get(""))  // use current directory
            .toAbsolutePath();
        return func.run(path);
    }
}
