package io.digdag.core.workflow;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandExecutorContent;
import io.digdag.spi.CommandStatus;
import io.digdag.spi.TaskRequest;

public class MockCommandExecutor
    implements CommandExecutor
{
    @Inject
    public MockCommandExecutor()
    { }

    @Override
    @Deprecated
    public Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
        throws IOException
    {
        return pb.start();
    }

    @Override
    public boolean test(TaskRequest request)
    {
        return false;
    }

    @Override
    public CommandStatus run(Path projectPath, Path workspacePath, TaskRequest request,
            Map<String, String> environments, List<String> cmdline,
            Map<String, CommandExecutorContent> inputContents,
            CommandExecutorContent outputContent)
            throws IOException, InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommandStatus poll(Path projectPath, Path workspacePath, TaskRequest request,
            String commandId, Config executorState)
    {
        throw new UnsupportedOperationException();
    }
}
