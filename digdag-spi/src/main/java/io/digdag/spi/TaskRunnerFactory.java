package io.digdag.spi;

import java.nio.file.Path;

public interface TaskRunnerFactory
{
    String getType();

    TaskRunner newTaskExecutor(Path archivePath, TaskRequest request);
}
