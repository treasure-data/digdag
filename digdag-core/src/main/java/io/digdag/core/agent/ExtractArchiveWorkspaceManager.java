package io.digdag.core.agent;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.commons.guava.ThrowablesUtil;
import io.digdag.core.TempFileManager.TempDir;
import io.digdag.core.TempFileManager;
import io.digdag.core.archive.ProjectArchives;
import io.digdag.spi.StorageObject;
import io.digdag.spi.TaskRequest;
import java.io.IOException;

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
                // Here has retrying because there were some observations that downloading of S3 caused too early EOF.
                // S3Storage uses ResumableInputStream but it can't rescue from too early end of streams.
                // Because ProjectResource.putProject validates the file, getting IOException here is always unexpected.
                //
                // A temporary workspace should be created for each try.
                // And the created temporary workspace should be closed finally.
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
                throw ThrowablesUtil.propagate(e.getCause());
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
        // prefix: {projectId}_{workflowName}_{attemptId}_{taskId}
        final String workspacePrefix = new StringBuilder()
                .append(request.getProjectId()).append("_")
                .append(request.getWorkflowName()).append("_") // workflow name is normalized before it's submitted.
                .append(request.getAttemptId()).append("_")
                .append(request.getTaskId())
                .toString();
        return tempFiles.createTempDir("workspace", workspacePrefix);
    }
}
