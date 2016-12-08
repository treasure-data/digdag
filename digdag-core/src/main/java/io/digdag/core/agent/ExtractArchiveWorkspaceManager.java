package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.core.TempFileManager.TempDir;
import io.digdag.core.TempFileManager;
import io.digdag.core.archive.ProjectArchives;
import io.digdag.spi.StorageObject;
import io.digdag.spi.TaskRequest;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractArchiveWorkspaceManager
    implements WorkspaceManager
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final TempFileManager tempFiles;

    @Inject
    public ExtractArchiveWorkspaceManager(TempFileManager tempFiles)
    {
        this.tempFiles = tempFiles;
    }

    @Override
    public <T> T withExtractedArchive(TaskRequest request, ArchiveProvider archiveProvider, WithWorkspaceAction<T> func)
            throws IOException
    {
        try (TempDir workspacePath = createNewWorkspace(request)) {
            Optional<StorageObject> in = archiveProvider.open();
            if (in.isPresent()) {
                ProjectArchives.extractTarArchive(workspacePath.get(), in.get().getContentInputStream());
            }
            return func.run(workspacePath.get());
        }
    }

    private TempDir createNewWorkspace(TaskRequest request)
        throws IOException
    {
        return tempFiles.createTempDir("workspace", request.getTaskName());
    }
}
