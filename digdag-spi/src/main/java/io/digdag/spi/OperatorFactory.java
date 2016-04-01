package io.digdag.spi;

import java.nio.file.Path;

public interface OperatorFactory
{
    String getType();

    Operator newTaskExecutor(Path workspacePath, TaskRequest request);
}
