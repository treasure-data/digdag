package io.digdag.spi;

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
     * Run command.
     *
     * @param projectPath
     * @param workspacePath
     * @param request
     * @param environments
     * @param cmdline
     * @param commandId
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    CommandStatus run(Path projectPath,
            Path workspacePath,
            TaskRequest request,
            Map<String, String> environments,
            List<String> cmdline,
            String commandId)
            throws IOException, InterruptedException;

    /**
     * Poll command task runnin and the status by non-blocking.
     *
     * @param projectPath
     * @param workspacePath
     * @param request
     * @param previousCommandStatus
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    CommandStatus poll(Path projectPath,
            Path workspacePath,
            TaskRequest request,
            CommandStatus previousCommandStatus)
            throws IOException, InterruptedException;
}