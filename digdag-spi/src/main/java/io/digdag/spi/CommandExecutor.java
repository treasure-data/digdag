package io.digdag.spi;

import io.digdag.client.config.Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface CommandExecutor
{
    @Deprecated
    Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
            throws IOException;

    CommandStatus run(Path projectPath,
            Path workspacePath,
            TaskRequest request,
            Map<String, String> environments,
            List<String> cmdline,
            Map<String, CommandExecutorContent> inputContents,   // {path => content}
            CommandExecutorContent outputContent)                // {content}
            throws IOException, InterruptedException;

    // this method is used for non-blocking task polling
    CommandStatus poll(Path projectPath,
            TaskRequest request,
            String commandId,
            Config executorState);
}