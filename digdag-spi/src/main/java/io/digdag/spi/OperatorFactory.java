package io.digdag.spi;

import java.nio.file.Path;

public interface OperatorFactory
{
    String getType();

    Operator newOperator(Path projectPath, TaskRequest request);
}
