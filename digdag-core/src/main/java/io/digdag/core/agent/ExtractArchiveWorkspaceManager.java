package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.digdag.core.TempFileManager.TempDir;
import io.digdag.core.TempFileManager;
import io.digdag.core.archive.ProjectArchives;
import io.digdag.spi.StorageObject;
import io.digdag.spi.TaskRequest;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractArchiveWorkspaceManager
    implements WorkspaceManager
{
    private static final int EXTRACT_RETRIES = 10;
    private static final int EXTRACT_MIN_RETRY_WAIT_MS = 1000;
    private static final int EXTRACT_MAX_RETRY_WAIT_MS = 30000;

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
        TempDir workspacePath = null;
        try {
            try {
                // A temporary workspace should be created for each try
                // And always close the temporary workspace
                workspacePath = RetryExecutor.retryExecutor()
                        .retryIf(exception -> true)
                        .withInitialRetryWait(EXTRACT_MIN_RETRY_WAIT_MS)
                        .withMaxRetryWait(EXTRACT_MAX_RETRY_WAIT_MS)
                        .onRetry((exception, retryCount, retryLimit, retryWait) ->
                                logger.warn("Failed to extract archive: retry {} of {}", retryCount, retryLimit, exception))
                        .withRetryLimit(EXTRACT_RETRIES)
                        .run(() -> {
                            TempDir newWorkSpacePath = null;
                            try {
                                newWorkSpacePath = createNewWorkspace(request);
                                Optional<StorageObject> in = archiveProvider.open();
                                if (in.isPresent()) {
                                    ProjectArchives.extractTarArchive(newWorkSpacePath.get(), in.get().getContentInputStream());
                                }
                                return newWorkSpacePath;
                            }
                            catch (Throwable e) {
                                if (newWorkSpacePath != null) {
                                    newWorkSpacePath.close();
                                }
                                throw e;
                            }
                        });
            }
            catch (RetryExecutor.RetryGiveupException e) {
                throw Throwables.propagate(e.getCause());
            }
            return func.run(workspacePath.get());
        }
        finally {
            if (workspacePath != null) {
                workspacePath.close();
            }
        }
    }

    private TempDir createNewWorkspace(TaskRequest request)
        throws IOException
    {
        return tempFiles.createTempDir("workspace", request.getTaskName());
    }
}
