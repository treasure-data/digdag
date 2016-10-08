package io.digdag.core.agent;

import java.nio.file.Path;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;

class DefaultOperatorContext
        implements OperatorContext
{
    private final Path projectPath;
    private final TaskRequest taskRequest;
    private final SecretProvider secretProvider;
    private final PrivilegedVariables privilegedVariables;

    DefaultOperatorContext(
            Path projectPath,
            TaskRequest taskRequest,
            SecretProvider secretProvider,
            PrivilegedVariables privilegedVariables)
    {
        this.projectPath = projectPath;
        this.taskRequest = taskRequest;
        this.secretProvider = secretProvider;
        this.privilegedVariables = privilegedVariables;
    }

    @Override
    public Path getProjectPath()
    {
        return projectPath;
    }

    @Override
    public TaskRequest getTaskRequest()
    {
        return taskRequest;
    }

    @Override
    public SecretProvider getSecrets()
    {
        return secretProvider;
    }

    @Override
    public PrivilegedVariables getPrivilegedVariables()
    {
        return privilegedVariables;
    }
}
