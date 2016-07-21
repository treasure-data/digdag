package io.digdag.core;

import io.digdag.client.config.Config;

import javax.inject.Inject;

public class Limits
{
    private static final long DEFAULT_MAX_WORKFLOW_TASKS = Long.valueOf(
            System.getProperty("io.digdag.limits.defaultMaxWorkflowTasks", "1000"));

    private final long maxWorkflowTasks;

    @Inject
    public Limits(Config systemConfig)
    {
        maxWorkflowTasks = systemConfig.get("limits.max-workflow-tasks", Long.class, DEFAULT_MAX_WORKFLOW_TASKS);
    }

    public long maxWorkflowTasks()
    {
        return maxWorkflowTasks;
    }
}
