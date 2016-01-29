package io.digdag.core.agent;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.FileSystems;
import com.google.inject.Inject;
import io.digdag.spi.TaskRequest;

public class CurrentDirectoryArchiveManager
    implements ArchiveManager
{
    @Inject
    public CurrentDirectoryArchiveManager()
    { }

    @Override
    public <T> T withExtractedArchive(TaskRequest request, WithArchiveAction<T> func)
    {
        Path archivePath = FileSystems.getDefault().getPath("").toAbsolutePath();
        return func.run(archivePath);
    }
}
