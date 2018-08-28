package io.digdag.core.workflow;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;

public class MockCommandExecutor
    implements CommandExecutor
{
    @Inject
    public MockCommandExecutor()
    { }

    public Process start(Path projectPath, TaskRequest request, ProcessBuilder pb)
        throws IOException
    {
        return pb.start();
    }
}
