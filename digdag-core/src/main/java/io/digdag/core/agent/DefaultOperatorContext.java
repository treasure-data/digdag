package io.digdag.core.agent;

import java.nio.file.Path;
import io.digdag.spi.PrivilegedVariables;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.OperatorContext;
import static java.util.Objects.requireNonNull;

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
        this.projectPath = requireNonNull(projectPath, "projectPath");
        this.taskRequest = requireNonNull(taskRequest, "taskRequest");
        this.secretProvider = requireNonNull(secretProvider, "secretProvider");
        this.privilegedVariables = requireNonNull(privilegedVariables, "privilegedVariables");
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
