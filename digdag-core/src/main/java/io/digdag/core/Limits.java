package io.digdag.core;

import io.digdag.client.config.Config;

import javax.inject.Inject;

public class Limits
{
    private static final long MAX_WORKFLOW_TASKS = Long.valueOf(
            System.getProperty("io.digdag.limits.maxWorkflowTasks", "1000"));

    private static final long MAX_ATTEMPTS = Long.valueOf(
            System.getProperty("io.digdag.limits.maxAttempts", "100"));

    private final Config systemConfig;

    @Inject
    public Limits(Config systemConfig)
    {
        this.systemConfig = systemConfig;
    }

    public long maxWorkflowTasks()
    {
        return systemConfig.get("executor.task_max_run", long.class, MAX_WORKFLOW_TASKS);
    }

    public long maxAttempts()
    {
        return systemConfig.get("executor.attempt_max_run", long.class, MAX_ATTEMPTS);
    }
}
