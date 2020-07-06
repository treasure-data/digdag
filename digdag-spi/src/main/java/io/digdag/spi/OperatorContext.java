package io.digdag.spi;

import java.nio.file.Path;

public interface OperatorContext
{
    Path getProjectPath();

    TaskRequest getTaskRequest();

    SecretProvider getSecrets();

    PrivilegedVariables getPrivilegedVariables();

    long getMaxWorkflowTasks();
}
