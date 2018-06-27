package io.digdag.spi;

import io.digdag.client.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface CommandExecutor
{
    // TODO Will be removed before 0.10.x 'rb' and 'sh' operator are still using it.
    @Deprecated
    Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    /**
     * Test if the command executor is available or not.
     *
     * @param request
     * @return
     */
    boolean test(TaskRequest request);

    /**
     * Run command.
     *
     * @param projectPath
     * @param workspacePath
     * @param request
     * @param environments
     * @param cmdline
     * @param inputContents
     * @param outputContent
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    CommandStatus run(Path projectPath,
            Path workspacePath,
            TaskRequest request,
            Map<String, String> environments,
            List<String> cmdline,
            Map<String, CommandExecutorContent> inputContents,   // {path => content}
            CommandExecutorContent outputContent)                // {content}
            throws IOException, InterruptedException;

    /**
     * Poll command task runnin and the status by non-blocking.
     *
     * @param projectPath
     * @param workspacePath
     * @param request
     * @param commandId
     * @param executorState
     * @return
     */
    CommandStatus poll(Path projectPath,
            Path workspacePath,
            TaskRequest request,
            String commandId,
            Config executorState);
}