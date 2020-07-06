package io.digdag.core.agent;

import java.nio.file.Path;

import io.digdag.core.Limits;
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
    private final long maxWorkflowTasks;

    DefaultOperatorContext(
            Path projectPath,
            TaskRequest taskRequest,
            SecretProvider secretProvider,
            PrivilegedVariables privilegedVariables,
            Limits limits)
    {
        this.projectPath = requireNonNull(projectPath, "projectPath");
        this.taskRequest = requireNonNull(taskRequest, "taskRequest");
        this.secretProvider = requireNonNull(secretProvider, "secretProvider");
        this.privilegedVariables = requireNonNull(privilegedVariables, "privilegedVariables");
        this.maxWorkflowTasks = limits.maxWorkflowTasks();
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

    @Override
    public long getMaxWorkflowTasks()
    {
        return maxWorkflowTasks;
    }
}
